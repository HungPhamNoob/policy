#!/usr/bin/env python3
"""
TomTom Traffic Incident producer.

Fetches TomTom Incident Details for configured US bbox regions, normalizes each
incident into the project raw Kafka event shape, and publishes it to the single
raw traffic topic consumed by the Flink inference job.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("STREAMING_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("tomtom-incident-producer")


DEFAULT_ENDPOINT = "https://api.tomtom.com/traffic/services/5/incidentDetails"
DEFAULT_FIELDS = (
    "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,"
    "magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,"
    "from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,"
    "numberOfReports,lastReportTime}}}"
)
DEFAULT_US_BBOXES = "US:New York:-74.25909,40.477399,-73.700181,40.917577"


def get_str_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()


def get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid int env %s=%r. Using default=%s", name, value, default)
        return default


def get_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning(
            "Invalid float env %s=%r. Using default=%s", name, value, default
        )
        return default


KAFKA_BOOTSTRAP_SERVERS = get_str_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = get_str_env("KAFKA_TOPIC_RAW", "traffic.us.raw")
PRODUCER_CLIENT_ID = get_str_env("PRODUCER_CLIENT_ID", "tomtom-incident-producer")

TOMTOM_ENDPOINT = get_str_env("TOMTOM_ENDPOINT", DEFAULT_ENDPOINT)
TOMTOM_API_KEY = get_str_env("TOMTOM_API_KEY", get_str_env("TOMTOM_API", ""))
TOMTOM_BBOX = get_str_env("TOMTOM_BBOX", "")
TOMTOM_BBOXES = get_str_env("TOMTOM_BBOXES", DEFAULT_US_BBOXES)
TOMTOM_LANGUAGE = get_str_env("TOMTOM_LANGUAGE", "en-US")
TOMTOM_TIME_VALIDITY = get_str_env("TOMTOM_TIME_VALIDITY", "present")
TOMTOM_FIELDS = get_str_env("TOMTOM_FIELDS", DEFAULT_FIELDS)
TOMTOM_REQUEST_TIMEOUT_SECONDS = get_float_env("TOMTOM_REQUEST_TIMEOUT_SECONDS", 20.0)
TOMTOM_POLL_SECONDS = get_float_env("TOMTOM_POLL_SECONDS", 60.0)
TOMTOM_MISSING_KEY_RETRY_SECONDS = get_float_env(
    "TOMTOM_MISSING_KEY_RETRY_SECONDS", 300.0
)
TOMTOM_RUN_ONCE = get_str_env("TOMTOM_RUN_ONCE", "false").lower() in {
    "1",
    "true",
    "yes",
}

STREAM_MAX_RECORDS = get_int_env("STREAM_MAX_RECORDS", 0)
PRODUCER_FLUSH_EVERY_N_RECORDS = get_int_env("PRODUCER_FLUSH_EVERY_N_RECORDS", 500)
PRODUCER_MAX_BUFFER_MESSAGES = get_int_env("PRODUCER_MAX_BUFFER_MESSAGES", 100000)
PRODUCER_LINGER_MS = get_int_env("PRODUCER_LINGER_MS", 50)
PRODUCER_BATCH_NUM_MESSAGES = get_int_env("PRODUCER_BATCH_NUM_MESSAGES", 10000)
PRODUCER_COMPRESSION_TYPE = get_str_env("PRODUCER_COMPRESSION_TYPE", "lz4")
PRODUCER_QUEUE_BACKOFF_SECONDS = get_float_env("PRODUCER_QUEUE_BACKOFF_SECONDS", 0.5)

STATE_SIGNATURE_FIELDS = (
    "timestamp",
    "last_report_time",
    "time_validity",
    "probability_of_occurrence",
    "icon_category",
    "delay_magnitude",
    "delay_seconds",
    "length_meters",
    "from_road",
    "to_road",
    "road_numbers",
    "geometry_wkt",
    "incident_code",
    "incident_description",
    "number_of_reports",
)


BboxRegion = Tuple[str, str, str]


def parse_bbox_regions() -> List[BboxRegion]:
    if TOMTOM_BBOX:
        return [("US", "Custom", TOMTOM_BBOX)]

    regions: List[BboxRegion] = []
    for item in TOMTOM_BBOXES.split(";"):
        parts = [part.strip() for part in item.split(":", 2)]
        if len(parts) != 3 or not all(parts):
            continue
        state_or_region, city, bbox = parts
        regions.append((state_or_region, city, bbox))
    return regions


def validate_config() -> None:
    if not TOMTOM_API_KEY:
        raise ValueError("Set TOMTOM_API or TOMTOM_API_KEY before starting producer")
    if not parse_bbox_regions():
        raise ValueError("Set TOMTOM_BBOX or TOMTOM_BBOXES with at least one bbox")
    if PRODUCER_FLUSH_EVERY_N_RECORDS <= 0:
        raise ValueError("PRODUCER_FLUSH_EVERY_N_RECORDS must be > 0")
    if STREAM_MAX_RECORDS < 0:
        raise ValueError("STREAM_MAX_RECORDS must be >= 0")
    if TOMTOM_POLL_SECONDS < 0:
        raise ValueError("TOMTOM_POLL_SECONDS must be >= 0")


def build_producer_config() -> Dict[str, Any]:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": PRODUCER_CLIENT_ID,
        "queue.buffering.max.messages": PRODUCER_MAX_BUFFER_MESSAGES,
        "linger.ms": PRODUCER_LINGER_MS,
        "batch.num.messages": PRODUCER_BATCH_NUM_MESSAGES,
        "compression.type": PRODUCER_COMPRESSION_TYPE,
        "acks": "all",
        "retries": 10,
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
    }


def first_coordinate(
    geometry: Dict[str, Any]
) -> Tuple[Optional[float], Optional[float]]:
    coordinates = geometry.get("coordinates")
    if not coordinates:
        return None, None

    point = coordinates
    while isinstance(point, list) and point and isinstance(point[0], list):
        point = point[0]

    if not isinstance(point, Sequence) or len(point) < 2:
        return None, None

    try:
        lon = float(point[0])
        lat = float(point[1])
    except (TypeError, ValueError):
        return None, None
    return lat, lon


def geometry_to_wkt(geometry: Dict[str, Any]) -> str:
    geometry_type = str(geometry.get("type", "")).lower()
    coordinates = geometry.get("coordinates")
    if geometry_type == "point" and isinstance(coordinates, Sequence):
        return f"POINT ({coordinates[0]} {coordinates[1]})"
    if geometry_type == "linestring" and isinstance(coordinates, list):
        points = []
        for coordinate in coordinates:
            if isinstance(coordinate, Sequence) and len(coordinate) >= 2:
                points.append(f"{coordinate[0]} {coordinate[1]}")
        if points:
            return f"LINESTRING ({', '.join(points)})"
    return ""


def normalize_incident(
    incident: Dict[str, Any],
    state_or_region: str,
    city: str,
) -> Optional[Dict[str, Any]]:
    properties = incident.get("properties") or {}
    geometry = incident.get("geometry") or {}
    incident_id = properties.get("id")
    if not incident_id:
        return None

    lat, lon = first_coordinate(geometry)
    if lat is None or lon is None:
        return None

    events = properties.get("events") or []
    first_event = events[0] if events else {}
    timestamp = properties.get("startTime") or properties.get("lastReportTime")
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()

    now = datetime.now(timezone.utc).isoformat()
    event_id = f"tomtom-{incident_id}"
    return {
        "event_id": event_id,
        "ID": event_id,
        "source": "tomtom",
        "incident_id": str(incident_id),
        "flow_segment_id": str(incident_id),
        "latitude": lat,
        "longitude": lon,
        "speed": 0.0,
        "timestamp": timestamp,
        "event_timestamp": timestamp,
        "state_or_region": state_or_region,
        "city": city,
        "icon_category": properties.get("iconCategory"),
        "delay_magnitude": properties.get("magnitudeOfDelay"),
        "delay_seconds": properties.get("delay"),
        "length_meters": properties.get("length"),
        "geometry_wkt": geometry_to_wkt(geometry),
        "incident_description": first_event.get("description"),
        "incident_code": first_event.get("code"),
        "from_road": properties.get("from"),
        "to_road": properties.get("to"),
        "road_numbers": properties.get("roadNumbers") or [],
        "time_validity": properties.get("timeValidity"),
        "probability_of_occurrence": properties.get("probabilityOfOccurrence"),
        "number_of_reports": properties.get("numberOfReports"),
        "last_report_time": properties.get("lastReportTime"),
        "ingestion_time": now,
        "_ingested_at_utc": now,
        "raw_payload": incident,
    }


def fetch_incidents(region: BboxRegion) -> Iterable[Dict[str, Any]]:
    state_or_region, city, bbox = region
    response = requests.get(
        TOMTOM_ENDPOINT,
        params={
            "bbox": bbox,
            "language": TOMTOM_LANGUAGE,
            "timeValidityFilter": TOMTOM_TIME_VALIDITY,
            "fields": TOMTOM_FIELDS,
            "key": TOMTOM_API_KEY,
        },
        timeout=TOMTOM_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    incidents = payload.get("incidents") or []
    logger.info(
        "Fetched %s TomTom incidents for %s/%s", len(incidents), state_or_region, city
    )
    for incident in incidents:
        normalized = normalize_incident(incident, state_or_region, city)
        if normalized is not None:
            yield normalized


def event_state_signature(event: Dict[str, Any]) -> str:
    payload = {field: event.get(field) for field in STATE_SIGNATURE_FIELDS}
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


delivery_success_count = 0
delivery_failed_count = 0


def delivery_report(error: Any, message: Any) -> None:
    global delivery_success_count, delivery_failed_count
    if error is not None:
        delivery_failed_count += 1
        logger.error("Kafka delivery failed: %s", error)
        return
    delivery_success_count += 1


def produce_with_backpressure(producer, topic: str, key: str, value: str) -> None:
    while True:
        try:
            producer.produce(
                topic=topic, key=key, value=value, callback=delivery_report
            )
            producer.poll(0)
            return
        except BufferError:
            logger.warning("Buffer full, backoff %.3fs", PRODUCER_QUEUE_BACKOFF_SECONDS)
            producer.poll(1.0)
            if PRODUCER_QUEUE_BACKOFF_SECONDS > 0:
                time.sleep(PRODUCER_QUEUE_BACKOFF_SECONDS)


def publish_event_if_changed(
    producer,
    topic: str,
    event: Dict[str, Any],
    last_seen_signatures: Dict[str, str],
) -> str:
    event_id = str(event["event_id"])
    signature = event_state_signature(event)
    previous_signature = last_seen_signatures.get(event_id)

    if previous_signature == signature:
        return "unchanged"

    value = json.dumps(event, ensure_ascii=False)
    produce_with_backpressure(producer, topic, event_id, value)
    last_seen_signatures[event_id] = signature
    if previous_signature is None:
        return "new"
    return "update"


def main() -> None:
    try:
        validate_config()
    except ValueError as exc:
        if "TOMTOM_API" not in str(exc):
            raise
        logger.error("%s", exc)
        logger.warning(
            "TomTom producer will stay idle instead of crash-looping. "
            "Inject TOMTOM_API_KEY and recreate the container to resume polling."
        )
        while True:
            time.sleep(max(TOMTOM_MISSING_KEY_RETRY_SECONDS, 30.0))

    regions = parse_bbox_regions()
    producer = Producer(build_producer_config())
    last_seen_signatures: Dict[str, str] = {}
    sent_rows = 0

    logger.info("=" * 80)
    logger.info("TomTom Incident Producer")
    logger.info("Kafka: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", KAFKA_TOPIC)
    logger.info("Regions: %s", regions)
    logger.info("=" * 80)

    try:
        while True:
            poll_fetched = 0
            poll_published_new = 0
            poll_published_updates = 0
            poll_skipped_unchanged = 0
            for region in regions:
                for event in fetch_incidents(region):
                    poll_fetched += 1
                    publish_status = publish_event_if_changed(
                        producer,
                        KAFKA_TOPIC,
                        event,
                        last_seen_signatures,
                    )
                    if publish_status == "unchanged":
                        poll_skipped_unchanged += 1
                        continue
                    if publish_status == "new":
                        poll_published_new += 1
                    else:
                        poll_published_updates += 1
                    sent_rows += 1
                    if sent_rows % PRODUCER_FLUSH_EVERY_N_RECORDS == 0:
                        producer.flush()
                    if STREAM_MAX_RECORDS > 0 and sent_rows >= STREAM_MAX_RECORDS:
                        producer.flush()
                        return

            producer.flush()
            logger.info(
                "TomTom poll summary: fetched=%s, published_new=%s, "
                "published_updates=%s, skipped_unchanged=%s, tracked_events=%s",
                poll_fetched,
                poll_published_new,
                poll_published_updates,
                poll_skipped_unchanged,
                len(last_seen_signatures),
            )
            if TOMTOM_RUN_ONCE:
                return
            time.sleep(TOMTOM_POLL_SECONDS)
    except KeyboardInterrupt:
        logger.warning("Interrupted")
    finally:
        producer.flush()
        logger.info("Done. Sent: %s", f"{sent_rows:,}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("Fatal producer error: %s", exc)
        sys.exit(1)
