#!/usr/bin/env python3
"""
ingestion/kafka/us_producer.py
US Accident Replay Producer – parallel raw Kafka publisher.

Reads the post-2020 US Accidents CSV from GCS (or the local filesystem) and
publishes each row as a raw JSON message to the Kafka topic `traffic.us.raw`.
Feature engineering is intentionally deferred to the Flink job so that the
producer remains a thin, stateless reader.

Parallel partitioning:
    Three producer instances share the dataset without overlap using modulo
    assignment on the 0-based row index:

        Producer 0 → row_index % TOTAL_PRODUCERS == 0
        Producer 1 → row_index % TOTAL_PRODUCERS == 1
        Producer 2 → row_index % TOTAL_PRODUCERS == 2

Input:
    gs://big-data-group-4-bronze/process/us_pipeline_from_2020.csv
    (or a local CSV path set via US_PIPELINE_REPLAY_PATH)

Output:
    Kafka topic: traffic.us.raw (multi-partition, replicated)
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

from dotenv import load_dotenv
from confluent_kafka import Producer

# Load a local .env file when the producer runs outside Docker Compose.
load_dotenv()

logging.basicConfig(
    level=os.getenv("STREAMING_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("us-replay-producer-raw")


# ---------------------------------------------------------------------------
# Environment variable helpers
# ---------------------------------------------------------------------------
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


def get_str_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()


# ---------------------------------------------------------------------------
# Configuration (resolved from environment variables)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = get_str_env(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = get_str_env(
    "KAFKA_TOPIC_RAW",
    "traffic.us.raw",
)
DATA_FILE_PATH = get_str_env(
    "US_PIPELINE_REPLAY_PATH",
    "data/process/us_pipeline_from_2020.csv",
)
STREAM_MAX_RECORDS = get_int_env("STREAM_MAX_RECORDS", 0)
STREAM_THROTTLE_SECONDS = get_float_env("STREAM_THROTTLE_SECONDS", 0.0)
STREAM_LOOP_FOREVER = get_str_env("STREAM_LOOP_FOREVER", "true").lower() in {
    "1",
    "true",
    "yes",
}
US_REPLAY_START_ROW = get_int_env("US_REPLAY_START_ROW", 0)
PRODUCER_CLIENT_ID = get_str_env("PRODUCER_CLIENT_ID", "us-replay-producer-raw")
PRODUCER_FLUSH_EVERY_N_RECORDS = get_int_env("PRODUCER_FLUSH_EVERY_N_RECORDS", 5000)

TOTAL_PRODUCERS = get_int_env(
    "TOTAL_PRODUCERS",
    get_int_env("PRODUCER_PARTITION", 1),
)
PRODUCER_INDEX = get_int_env(
    "PRODUCER_INDEX",
    get_int_env("PRODUCER_PARTITION_INDEX", 0),
)

PRODUCER_MAX_BUFFER_MESSAGES = get_int_env("PRODUCER_MAX_BUFFER_MESSAGES", 100000)
PRODUCER_LINGER_MS = get_int_env("PRODUCER_LINGER_MS", 50)
PRODUCER_BATCH_NUM_MESSAGES = get_int_env("PRODUCER_BATCH_NUM_MESSAGES", 10000)
PRODUCER_COMPRESSION_TYPE = get_str_env("PRODUCER_COMPRESSION_TYPE", "lz4")
PRODUCER_QUEUE_BACKOFF_SECONDS = get_float_env("PRODUCER_QUEUE_BACKOFF_SECONDS", 0.5)


# ---------------------------------------------------------------------------
# File I/O – GCS and local filesystem support
# ---------------------------------------------------------------------------


def open_file(path: str, mode: str = "r"):
    """
    Open a source CSV from GCS or the local filesystem.

    GCS paths (prefixed with 'gs://') are accessed via gcsfs.
    All other paths are opened with the standard Python open() function.
    """
    if path.startswith("gs://"):
        import gcsfs

        fs = gcsfs.GCSFileSystem()
        logger.info("Reading from GCS: %s", path)
        # gcsfs expects the bucket path without the gs:// prefix.
        bucket_path = path.replace("gs://", "", 1)
        return fs.open(bucket_path, mode=mode, encoding="utf-8")
    else:
        logger.info("Reading local file: %s", path)
        return open(path, mode=mode, encoding="utf-8", newline="")


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------
def validate_config() -> None:
    if TOTAL_PRODUCERS <= 0:
        raise ValueError("TOTAL_PRODUCERS must be >= 1")
    if PRODUCER_INDEX < 0 or PRODUCER_INDEX >= TOTAL_PRODUCERS:
        raise ValueError(
            f"PRODUCER_INDEX must be in range [0, {TOTAL_PRODUCERS - 1}], "
            f"got PRODUCER_INDEX={PRODUCER_INDEX}"
        )
    if PRODUCER_FLUSH_EVERY_N_RECORDS <= 0:
        raise ValueError("PRODUCER_FLUSH_EVERY_N_RECORDS must be > 0")
    if STREAM_MAX_RECORDS < 0:
        raise ValueError("STREAM_MAX_RECORDS must be >= 0")
    if STREAM_THROTTLE_SECONDS < 0:
        raise ValueError("STREAM_THROTTLE_SECONDS must be >= 0")
    if US_REPLAY_START_ROW < 0:
        raise ValueError("US_REPLAY_START_ROW must be >= 0")
    if PRODUCER_QUEUE_BACKOFF_SECONDS < 0:
        raise ValueError("PRODUCER_QUEUE_BACKOFF_SECONDS must be >= 0")


# ---------------------------------------------------------------------------
# Kafka producer configuration
# ---------------------------------------------------------------------------
def build_producer_config() -> Dict[str, Any]:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": f"{PRODUCER_CLIENT_ID}-{PRODUCER_INDEX}",
        "queue.buffering.max.messages": PRODUCER_MAX_BUFFER_MESSAGES,
        "linger.ms": PRODUCER_LINGER_MS,
        "batch.num.messages": PRODUCER_BATCH_NUM_MESSAGES,
        "compression.type": PRODUCER_COMPRESSION_TYPE,
        "acks": "all",
        "retries": 10,
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
    }


# ---------------------------------------------------------------------------
# Kafka delivery callback
# ---------------------------------------------------------------------------
delivery_success_count = 0
delivery_failed_count = 0


def delivery_report(error: Any, message: Any) -> None:
    global delivery_success_count, delivery_failed_count
    if error is not None:
        delivery_failed_count += 1
        logger.error("Kafka delivery failed: %s", error)
        return
    delivery_success_count += 1


# ---------------------------------------------------------------------------
# Core producer logic
# ---------------------------------------------------------------------------
def should_send_row(row_index: int) -> bool:
    return row_index % TOTAL_PRODUCERS == PRODUCER_INDEX


def produce_with_backpressure(producer, topic, key, value) -> None:
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


def print_startup_log() -> None:
    logger.info("=" * 80)
    logger.info("US Accident Replay Producer - RAW Parallel (GCS Native)")
    logger.info("Kafka: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", KAFKA_TOPIC)
    logger.info("CSV:   %s", DATA_FILE_PATH)
    logger.info("Producer: %s/%s", PRODUCER_INDEX + 1, TOTAL_PRODUCERS)
    logger.info("Loop forever: %s", STREAM_LOOP_FOREVER)
    logger.info("Replay start row: %s", f"{US_REPLAY_START_ROW:,}")
    logger.info("=" * 80)


def stream_dataset_once(
    producer: Producer,
    total_sent_so_far: int,
    overall_start_time: float,
    cycle_number: int,
) -> tuple[int, int, int]:
    """Publish one full pass of the replay dataset for this producer shard."""
    scanned_rows = 0
    skipped_rows = 0
    sent_rows = 0

    with open_file(DATA_FILE_PATH) as f:
        reader = csv.DictReader(f)
        logger.info("Cycle %s CSV columns: %s", cycle_number, reader.fieldnames)

        for row_index, row in enumerate(reader):
            scanned_rows += 1

            if row_index < US_REPLAY_START_ROW:
                skipped_rows += 1
                continue

            if not should_send_row(row_index):
                skipped_rows += 1
                continue

            key = row.get("ID") or f"row-{row_index}"
            row["_ingested_at_utc"] = datetime.now(timezone.utc).isoformat()
            row["_replay_cycle"] = cycle_number
            row["_replay_row_index"] = row_index
            row["_replay_producer_index"] = PRODUCER_INDEX
            row["_replay_total_producers"] = TOTAL_PRODUCERS
            row["_replay_emission_id"] = (
                f"c{cycle_number}-p{PRODUCER_INDEX}-r{row_index}"
            )
            value = json.dumps(row, ensure_ascii=False)

            produce_with_backpressure(producer, KAFKA_TOPIC, key, value)
            sent_rows += 1
            total_sent = total_sent_so_far + sent_rows

            if total_sent % 1000 == 0:
                elapsed = max(time.time() - overall_start_time, 1e-6)
                logger.info(
                    "producer=%s cycle=%s sent_total=%s sent_cycle=%s scanned_cycle=%s skipped_cycle=%s rate=%.0f rows/s",
                    PRODUCER_INDEX,
                    cycle_number,
                    f"{total_sent:,}",
                    f"{sent_rows:,}",
                    f"{scanned_rows:,}",
                    f"{skipped_rows:,}",
                    total_sent / elapsed,
                )

            if total_sent % PRODUCER_FLUSH_EVERY_N_RECORDS == 0:
                producer.flush()

            if STREAM_THROTTLE_SECONDS > 0:
                time.sleep(STREAM_THROTTLE_SECONDS)

            if STREAM_MAX_RECORDS > 0 and total_sent >= STREAM_MAX_RECORDS:
                break

    return scanned_rows, skipped_rows, sent_rows


def main() -> None:
    validate_config()
    print_startup_log()

    producer = Producer(build_producer_config())
    total_scanned_rows = 0
    total_skipped_rows = 0
    total_sent_rows = 0
    cycle_number = 0
    start_time = time.time()

    try:
        while True:
            cycle_number += 1
            logger.info("Starting replay cycle %s for producer shard %s.", cycle_number, PRODUCER_INDEX)
            scanned_rows, skipped_rows, sent_rows = stream_dataset_once(
                producer=producer,
                total_sent_so_far=total_sent_rows,
                overall_start_time=start_time,
                cycle_number=cycle_number,
            )
            total_scanned_rows += scanned_rows
            total_skipped_rows += skipped_rows
            total_sent_rows += sent_rows
            producer.flush()

            logger.info(
                "Completed replay cycle %s. cycle_sent=%s total_sent=%s",
                cycle_number,
                f"{sent_rows:,}",
                f"{total_sent_rows:,}",
            )

            if STREAM_MAX_RECORDS > 0 and total_sent_rows >= STREAM_MAX_RECORDS:
                break
            if not STREAM_LOOP_FOREVER:
                break

            time.sleep(2)
    except KeyboardInterrupt:
        logger.warning("Interrupted")
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        raise
    finally:
        producer.flush()
        elapsed = max(time.time() - start_time, 1e-6)
        logger.info("=" * 80)
        logger.info(
            "Done. Sent: %s | Skipped: %s | Rate: %.0f rows/s",
            f"{total_sent_rows:,}",
            f"{total_skipped_rows:,}",
            total_sent_rows / elapsed,
        )
        logger.info("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("Fatal producer error: %s", exc)
        sys.exit(1)
