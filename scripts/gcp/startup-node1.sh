#!/bin/bash
# scripts/gcp/startup-node1.sh - runs automatically when the VM starts.

set -e

echo "Node 1 startup script running..."

ensure_apt_packages() {
  local missing_packages=()
  local package_name
  for package_name in "$@"; do
    if ! dpkg -s "${package_name}" >/dev/null 2>&1; then
      missing_packages+=("${package_name}")
    fi
  done

  if [ "${#missing_packages[@]}" -eq 0 ]; then
    return 0
  fi

  echo "Installing missing operating-system packages: ${missing_packages[*]}"
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${missing_packages[@]}"
}

ensure_apt_packages \
  ca-certificates \
  curl \
  git \
  openjdk-17-jre-headless \
  python3 \
  python3-pip \
  python3-venv

# Install Docker
if ! command -v docker &> /dev/null; then
  echo "Installing Docker..."
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  TARGET_USER="${SUDO_USER:-${USER:-}}"
  if [ -n "${TARGET_USER}" ]; then
    usermod -aG docker "${TARGET_USER}" || true
  fi
  rm get-docker.sh
fi

# Install Docker Compose v2
if ! docker compose version &> /dev/null; then
  echo "Installing Docker Compose..."
  DOCKER_CONFIG=${DOCKER_CONFIG:-/usr/local/lib/docker}
  mkdir -p "${DOCKER_CONFIG}/cli-plugins"
  curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 -o "${DOCKER_CONFIG}/cli-plugins/docker-compose"
  chmod +x "${DOCKER_CONFIG}/cli-plugins/docker-compose"
fi

if ! command -v gcloud &> /dev/null; then
  echo "Installing gcloud CLI..."
  curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  tar -xf google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  ./google-cloud-sdk/install.sh --quiet
  echo 'export PATH="$PATH:$HOME/google-cloud-sdk/bin"' >> ~/.bashrc
fi

echo "Preparing /opt/traffic directory..."
mkdir -p /opt/traffic
chmod 775 /opt/traffic

REPO_URL="${REPO_URL:-https://github.com/HungPhamNoob/traffic-risk-assessment.git}"
if [ ! -d /opt/traffic/.git ]; then
  echo "Cloning the latest repository snapshot into /opt/traffic."
  rm -rf /opt/traffic
  git clone "${REPO_URL}" /opt/traffic
else
  echo "Refreshing /opt/traffic from ${REPO_URL}."
  cd /opt/traffic
  git config --global --add safe.directory /opt/traffic 2>/dev/null || true
  git fetch --prune origin || true
  git pull --ff-only origin main || true
fi

GCS_ENV_PATH="${GCS_ENV_PATH:-gs://big-data-group-4-bronze/env/.env.cloud}"
if [ -f /opt/traffic/.env.cloud ]; then
  detected_gcs_env_path="$(awk -F= '/^GCS_ENV_PATH=/{print $2; exit}' /opt/traffic/.env.cloud 2>/dev/null || true)"
  if [ -n "${detected_gcs_env_path}" ]; then
    GCS_ENV_PATH="${detected_gcs_env_path}"
  fi
fi
echo "Refreshing runtime env from ${GCS_ENV_PATH} when available."
gcloud storage cp "${GCS_ENV_PATH}" /opt/traffic/.env.cloud >/dev/null 2>&1 || true
cp /opt/traffic/.env.cloud /opt/traffic/.env 2>/dev/null || true

if ! command -v uv &> /dev/null; then
  echo "Installing uv for local Python workflow checks..."
  curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh
fi

# Configure Docker for Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

AUTO_START_NODE_SERVICES="${AUTO_START_NODE_SERVICES:-true}"
STARTUP_LOG_DIR="/var/log/traffic"

if [ "${AUTO_START_NODE_SERVICES}" = "true" ]; then
  if [ -f /opt/traffic/.env.cloud ] && [ -x /opt/traffic/scripts/gcp/run-node1.sh ]; then
    echo "Starting Node 1 services using run-node1.sh."
    mkdir -p "${STARTUP_LOG_DIR}"
    nohup bash -c "cd /opt/traffic && bash scripts/gcp/run-node1.sh" \
      >"${STARTUP_LOG_DIR}/node1-bootstrap.log" 2>&1 &
  else
    echo "Skipping Node 1 service bootstrap (missing /opt/traffic/.env.cloud or run-node1.sh)."
  fi
fi

echo "Node 1 startup complete."
