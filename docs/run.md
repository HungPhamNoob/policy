# GCP Cloud Run Guide A-Z

This guide is the cloud-first runbook for `big-data-group-4`. It is written for a new student who wants to run the complete Big Data project without understanding every internal service first.

The project uses three Google Compute Engine VMs:

- `node1-control`: PostgreSQL/PostGIS, Airflow, MLflow, FastAPI, Prometheus, and Grafana.
- `node2-streaming`: Kafka, Flink streaming inference, Redis, and replay producers.
- `node3-batch`: Spark Silver-to-Gold batch processing and H2O retraining.

The expected data flow is:

```text
US before 2020 -> H2O/MLflow baseline
US from 2020 -> Node2 Kafka/Flink -> Silver GCS + Postgres -> Node3 Spark -> Gold GCS -> H2O/MLflow
TomTom live -> Node2 Kafka/Flink -> Postgres
Postgres + MLflow -> FastAPI JSON -> Dashboard
```

## Public URLs

Current VM addresses from the latest provided inventory:

| Service | URL |
| --- | --- |
| Dashboard | `http://35.224.149.110:3001` |
| FastAPI docs | `http://35.224.149.110:8000/docs` |
| Airflow | `http://35.224.149.110:8080` |
| MLflow | `http://35.224.149.110:5000` |
| Grafana | `http://35.224.149.110:3000` |
| Prometheus | `http://35.224.149.110:9090` |
| Flink JobManager | `http://34.46.107.159:8081` |
| Spark Master | `http://34.63.78.147:8080` |

Credentials from `.env.cloud` defaults:

- Airflow: `admin` / `123`
- Grafana: `admin` / `123`

If an external IP changes, regenerate the table with:

```bash
make -f makefile/gcp/Makefile collect-metrics
```

The end-to-end cloud runner also stores service request/response evidence in:

```text
logs/cloud_runs/<run-id>/service-checks.md
```

## 1. Local Machine Rules

Run commands from the repository root.

Use local only for static checks. Do not start the full local Docker Compose stack on a weak laptop unless the teacher explicitly asks for local demo evidence.

```bash
gcloud auth login
gcloud config set project big-data-group-4
gcloud config set compute/zone us-central1-a

gcloud auth list
gcloud config list
```

Safe local checks:

```bash
python3 -m pytest tests/ -v --tb=short
python3 -m compileall dashboard/backend/app processing ingestion ml/dataset ml/training orchestration -q

docker compose --env-file .env.example -f docker-compose.yaml config --quiet
docker compose --env-file .env.cloud -f deployment/node1-control/docker-compose.yaml config --quiet
docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml config --quiet
docker compose --env-file .env.cloud -f deployment/node3-batch/docker-compose.yaml config --quiet
```

If you need a full, real run, always use the cloud pipeline instead of local smoke tests.

If local Docker containers were started accidentally, stop them immediately:

```bash
docker compose --file docker-compose.yaml --env-file .env --project-name traffic-local down --timeout 5
```

If Docker Snap returns `permission denied` while stopping containers, repair AppArmor and retry:

```bash
sudo aa-remove-unknown
sudo systemctl restart snapd.apparmor
sudo systemctl reload apparmor 2>/dev/null || sudo systemctl restart apparmor

docker compose --file docker-compose.yaml --env-file .env --project-name traffic-local down --timeout 5
```

## 2. Bootstrap GCP

Create or repair service account, buckets, Artifact Registry, firewall rules, and all three VMs:

```bash
export GCP_PROJECT_ID=big-data-group-4
export GCP_ZONE=us-central1-a
export GCP_REGION=us-central1

bash scripts/gcp/setup_gcp.sh
```

Upload Bronze input data if the bucket is empty:

```bash
bash scripts/gcp/upload_data_to_gcs.sh
```

Validate cloud resources:

```bash
gcloud compute instances list --project="$GCP_PROJECT_ID"
gcloud storage buckets list --project="$GCP_PROJECT_ID"
gcloud artifacts repositories list --location="$GCP_REGION" --project="$GCP_PROJECT_ID"
```

Required buckets:

- `big-data-group-4-bronze`
- `big-data-group-4-silver`
- `big-data-group-4-gold`
- `big-data-group-4-backups`
- `big-data-group-4-ml-artifacts`

## 3. Cloud Environment File

`.env.cloud` is the runtime configuration source for all VMs. VM internal IPs can change when VMs are recreated, so always verify them before manual deploy:

```bash
gcloud compute instances list \
  --filter='name~node[123]' \
  --format='table(name,status,networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)'
```

Update these values when IPs change:

```bash
NODE1_INTERNAL_IP=<node1-internal-ip>
NODE2_INTERNAL_IP=<node2-internal-ip>
NODE3_INTERNAL_IP=<node3-internal-ip>
POSTGRES_HOST=<node1-internal-ip>
MLFLOW_TRACKING_URI=http://<node1-internal-ip>:5000
MLFLOW_SERVING_ENDPOINT=http://<node1-internal-ip>:5001/invocations
FASTAPI_IMAGE=us-central1-docker.pkg.dev/big-data-group-4/capstone/fastapi:latest
POSTGRES_PREDICTION_TABLE=traffic_risk_predictions
```

To speed up the end-to-end loop during demonstrations, reduce Airflow schedules in `.env.cloud`:

```bash
AIRFLOW_MODEL_RETRAIN_SCHEDULE=*/5 * * * *
AIRFLOW_STREAM_HEALTH_SCHEDULE=*/2 * * * *
```

These defaults are safe to override later for production.

Upload the environment file for deploy scripts:

```bash
gcloud storage cp .env.cloud gs://big-data-group-4-bronze/env/.env.cloud
```

## 4. Build FastAPI Image

GitHub Actions normally builds and pushes this image. Manual fallback:

```bash
gcloud auth configure-docker us-central1-docker.pkg.dev

docker build -t us-central1-docker.pkg.dev/big-data-group-4/capstone/fastapi:latest \
  -f dashboard/backend/Dockerfile \
  dashboard/backend

docker push us-central1-docker.pkg.dev/big-data-group-4/capstone/fastapi:latest
```

The VM service account must be allowed to pull images:

```bash
gcloud projects add-iam-policy-binding big-data-group-4 \
  --member="serviceAccount:team4-sa@big-data-group-4.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader" \
  --condition=None
```

## 5. Manual Cloud Deploy

Use this when GitHub Actions is unavailable or when debugging a VM directly. Deploy Node 1 first, then Node 2, then Node 3.

Use the same safe Git sync pattern on every VM. It handles first-time clone and old startup-script copies that do not contain `.git`, but it avoids destructive resets when the deployment worktree is already dirty:

```bash
cd /opt
if [ ! -d /opt/traffic/.git ]; then
  sudo rm -rf /opt/traffic
  sudo mkdir -p /opt/traffic
  sudo chown -R "$(whoami):$(whoami)" /opt/traffic
  git clone https://github.com/HungPhamNoob/traffic-risk-assessment.git /opt/traffic
fi

cd /opt/traffic
git config --global --add safe.directory /opt/traffic 2>/dev/null || true
git fetch --prune origin
git status --short
git pull --ff-only origin main
```

Then run the node-specific command.

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  bash scripts/gcp/run-node1.sh
'
```

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  bash scripts/gcp/run-node2.sh
'
```

```bash
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  NODE3_H2O_MAX_RUNTIME=120 bash scripts/gcp/run-node3.sh
'
```

Restart only the Flink Python job after changing streaming code:

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml up -d --force-recreate flink-python-job
'
```

For a safer runtime-config rollout on node2, prefer the controlled restart helper.
It cancels the current Flink job through the REST API, waits for zero running jobs,
then recreates only the submitter container and waits for exactly one replacement
job to return:

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  bash scripts/gcp/restart-node2-flink-job.sh
'
```

## 6. GitHub Actions Deploy

Workflow: `.github/workflows/ci-cd.yaml`.

It performs these steps on every push to `main`:

- Run Black, Flake8, unit tests, Python compile checks, and Docker Compose validation.
- Ensure Artifact Registry repository `capstone` exists.
- Build and push the FastAPI image.
- Ensure GCP buckets, service account, firewall rules, and the three VMs exist.
- Install the deploy SSH public key into VM metadata.
- Upload `.env.cloud` to GCS with current VM internal IP overrides.
- Deploy Node 1, optionally Node 2 and Node 3, then run smoke tests.

Required GitHub secrets:

- `GCP_PROJECT_ID=big-data-group-4`
- `GCP_SA_KEY`: JSON key for a service account that can manage Compute Engine, Storage, Artifact Registry, and existing VM metadata. API enabling should be done once by a project owner; CI treats API enable as best-effort.
- `ENV_CLOUD`: complete `.env.cloud` content.
- `HUNG_SSH_PRIVATE_KEY`: private key matching the inter-node deploy key installed on the VMs.

Optional GitHub variables:

- `HUNG_SSH_USER=runner`
- `DEPLOY_STREAMING=true`
- `DEPLOY_BATCH=true`

`HUNG_SSH_USER` must match the private key stored in `HUNG_SSH_PRIVATE_KEY`.
The current CI/CD flow copies that private key into Node 1 and Airflow uses it
for inter-node SSH to Node 3 during retraining. If the user/key pair does not
match, `model_retrain_hourly` will stay in `up_for_retry` or `failed` because
Airflow cannot SSH into Node 3.

If CI fails with `node3-batch not found`, run:

```bash
GCP_PROJECT_ID=big-data-group-4 GCP_ZONE=us-central1-a GCP_REGION=us-central1 bash scripts/gcp/setup_gcp.sh
```

Then push again to `main`.

## 7. Full Cloud Pipeline Validation

For a real end-to-end run from a clean state, prefer the automated command:

```bash
BRANCH=main STREAM_MAX_RECORDS=0 STREAM_THROTTLE_SECONDS=0.002 \
make -f makefile/gcp/Makefile full-reset-run
```

Reset and run the realtime-only branch from the beginning:

```bash
make -f makefile/gcp/Makefile full-reset-run-realtime
```

The full reset runner automatically:

1. Packages the current workspace and uploads it to all three VMs.
2. Writes a run-specific `.env.cloud` with clean Flink, Spark, Silver, and Gold prefixes.
3. Resets PostgreSQL realtime tables, Kafka/Flink state, Spark local state, and the run-specific GCS prefixes.
4. Retrains the pre-2020 baseline again when `RUN_OFFLINE_TRAINING=true`.
5. Starts replay, TomTom live ingestion, Spark, H2O retraining, metrics collection, and service request/response checks.

This command:

- Creates run-specific GCS prefixes for Flink checkpoints, Spark checkpoints, Silver features, and Gold retrain outputs.
- Resets PostgreSQL serving tables and Kafka/Flink/Spark local Docker volumes.
- Starts Node 1 and runs or verifies pre-2020 H2O model bootstrap.
- Starts Node 2 with full US replay and TomTom live ingestion.
- Starts Node 3 Spark Silver-to-Gold and H2O retraining.
- Collects measured evidence into `logs/cloud_runs/<run-id>/cloud-metrics.md`.

Use `STREAM_MAX_RECORDS=0` for a real full replay. Set a positive value only for debugging.

### 7.1 Check Node 1 API and databases

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  curl -fsS http://localhost:8000/health && echo &&
  curl -fsS http://localhost:8000/api/v1/system/status | python3 -m json.tool &&
  docker exec node1-postgres psql -U capstone -d capstone_db \
    -c "SELECT count(*) AS us_predictions, max(event_time) AS latest_us_time FROM traffic_risk_predictions;" &&
  docker exec node1-postgres psql -U capstone -d capstone_db \
    -c "SELECT count(*) AS tomtom_incidents, max(event_time) AS latest_tomtom_time FROM traffic_tomtom_incidents;"
'
```

### 7.2 Train the before-2020 model

Use a bounded runtime during debugging. For final evidence, increase `H2O_MAX_RUNTIME` if the VM budget allows it.

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  docker compose --env-file .env.cloud -f deployment/node1-control/docker-compose.yaml ps mlflow &&
  IS_TRAIN_OFFLINE=true H2O_MAX_RUNTIME=300 bash scripts/gcp/run-node1.sh
'
```

Expected evidence:

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  curl -fsS http://localhost:5000/health && echo MLflow_OK
'
```

### 7.3 Run streaming replay

Node 2 starts long-running Kafka, producer, and Flink containers in the
background. The first container logs include dependency installation because
the streaming services use lightweight base images; the replay starts after
those dependencies are installed.

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  STREAM_MAX_RECORDS=900 STREAM_THROTTLE_SECONDS=0.002 bash scripts/gcp/run-node2.sh &&
  docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml logs --tail=120 flink-python-job
'
```

Expected evidence:

```bash
gcloud storage ls "gs://big-data-group-4-silver/process/flink_features/**" | head
```

### 7.4 Run Spark Silver-to-Gold and H2O retraining

Node 3 waits for at least one Silver feature object before running Spark. This
prevents a successful but empty Spark job when Node 3 starts before Flink has
written its first GCS object.

```bash
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  NODE3_WAIT_FOR_SILVER_SECONDS=600 SPARK_READ_PARTITIONS=64 NODE3_H2O_MAX_RUNTIME=300 bash scripts/gcp/run-node3.sh
'
```

Expected evidence:

```bash
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  find data/cloud/gold/features/retrain -maxdepth 4 -type f | head
'
```

If Node 2 is still actively writing Silver data, Node 3 may print transient `GcsNotFoundError` or rsync warnings while copying the moving snapshot. The run script continues with the files already copied, and Spark is configured to ignore missing/corrupt files for this active-streaming race.

### 7.5 Collect Backend JSON outputs

```bash
for path in \
  /api/v1/system/status \
  /api/v1/overview/summary \
  /api/v1/predictions/map?limit=20 \
  /api/v1/hotspots?limit=10\&min_events=1 \
  /api/v1/analytics/risk-by-hour \
  /api/v1/analytics/severity-distribution
 do
  gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command="
    curl -fsS -H 'Accept: application/json' http://localhost:8000${path} | python3 -m json.tool >/tmp/api-output.json &&
    echo OK ${path} &&
    head -40 /tmp/api-output.json
  "
done
```

## 8. Metrics Evidence

After the pipeline has been running, collect real measured values:

```bash
make -f makefile/gcp/Makefile collect-metrics
```

The generated markdown includes:

- Producer throughput samples from Kafka producer logs.
- API throughput and p95 latency from Prometheus.
- End-to-end latency from PostgreSQL `end_to_end_latency_ms`.
- Row counts and latest event timestamps for `traffic_risk_predictions` and `traffic_tomtom_incidents`.
- Kafka topic offsets for `traffic.us.raw` and `traffic.tomtom.raw`.
- Docker service status across all VMs.

Do not report throughput or latency numbers in the final report until they come from this evidence file.

## 9. Reset and Retry

Reset replay/checkpoint state before a clean streaming demo:

```bash
make -f makefile/gcp/Makefile reset-realtime
```

Replay the US dataset again from the beginning of the 2020 split:

```bash
make -f makefile/gcp/Makefile full-reset-run-realtime
```

That command resets PostgreSQL realtime tables, Kafka/Flink state, Spark local
state, Silver/Gold retrain outputs, and then restarts Node 2 plus Node 3 so
the replay starts again from row 0 of `us_pipeline_from_2020.csv`.

Restart Node 2 and Node 3 as a pair:

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='cd /opt/traffic && bash scripts/gcp/run-node2.sh'
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='cd /opt/traffic && NODE3_WAIT_FOR_SILVER_SECONDS=600 bash scripts/gcp/run-node3.sh'
```

## 10. Common Failures

- `node3-batch not found`: run `scripts/gcp/setup_gcp.sh`; the current script creates all three VMs.
- Node 1 disk 100%: resize boot disk, then run `sudo growpart /dev/sda 1 && sudo resize2fs /dev/sda1` on the VM.
- Docker pull from Artifact Registry fails: grant `roles/artifactregistry.reader` to `team4-sa@big-data-group-4.iam.gserviceaccount.com`.
- Airflow unhealthy after restart: the compose command uses `airflow db migrate || airflow db init`; check `docker logs node1-airflow`.
- Airflow UI unreachable on `:8080` while scheduler still runs: check both `node1-airflow` and `node1-airflow-scheduler`. The webserver now runs in `node1-airflow`; the scheduler runs separately in `node1-airflow-scheduler`.
- MLflow experiment history disappears after a repo refresh: verify that Node 1 is using the persistent Docker volume `mlflow_data` instead of a bind mount inside `/opt/traffic`.
- Flink import fails: verify `PYTHONPATH=/opt/traffic` and that `processing.feature_engineering` exists on the VM.
- Backend returns empty JSON: this is valid if `traffic_risk_predictions` has no rows yet. Run streaming replay first.
- Retraining stays `queued`, `up_for_retry`, or `failed`: verify that `HUNG_SSH_USER` matches the private key mounted at `/run/secrets/google_compute_engine` inside `node1-airflow`, then test `ssh -i /run/secrets/google_compute_engine ${HUNG_SSH_USER}@${NODE3_INTERNAL_IP} hostname` from the container.
- Local Docker cannot stop containers: repair Snap AppArmor as shown in Section 1.

## 11. Git Ignore Notes

`.gitignore` only prevents new untracked files from being added. It does not remove files that are already tracked in Git history.

This repository intentionally tracks a few files that look like they could be ignored:

- `.env.cloud` is explicitly re-included by `!.env.cloud` because it is used as a cloud deployment template.
- `vendor/baseline_*.md` are explicitly re-included as lightweight reference summaries.

To remove an already tracked generated file from Git while keeping the local copy:

```bash
git rm --cached <path>
git commit -m "Remove generated file from tracking"
```

Do not commit private keys, service-account JSON files, raw datasets, local virtual environments, Docker volumes, logs, or model artifacts.

## 12. Operating Principles

- Deploy Node 1 before Node 2 and Node 3.
- Treat Node 2 and Node 3 as a pair after replay/checkpoint failures.
- FastAPI must always return valid JSON, even when the prediction table is empty.
- Keep private keys and service-account JSON only in GitHub Secrets or local `~/.ssh`/secure secret storage.
- Rotate any private key that has been pasted into chat, terminal logs, or documentation.
