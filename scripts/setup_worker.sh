#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  -s, --server URL           Backend base URL (default: http://localhost:8000)
  -w, --worker-id ID         Worker identifier (default: hostname)
  -n, --nfs-server HOST      NFS server host or IP (required)
  -e, --nfs-export PATH      NFS export path on the server (default: /opt/opeva_shared_data)
  -m, --mount-point PATH     Local mount point (default: /opt/opeva_shared_data)
  -i, --image NAME[:TAG]     Worker image (default: calof/job_worker_agent:latest)
  -p, --poll FLOAT           Queue poll interval in seconds (default: 5)
  -h, --heartbeat FLOAT      Heartbeat interval in seconds (default: 30)
  --status-poll FLOAT        Status poll interval in seconds (default: 10)
  --dry-run                  Show commands without executing
  --help                     Show this help and exit

Example:
  sudo $0 \
    --server http://backend:8000 \
    --worker-id worker-a \
    --nfs-server 10.0.0.5 \
    --nfs-export /opt/opeva_shared_data
USAGE
}

DRY_RUN=false
SERVER_URL="http://localhost:8000"
WORKER_ID="$(hostname)"
NFS_SERVER=""
NFS_EXPORT="/opt/opeva_shared_data"
MOUNT_POINT="/opt/opeva_shared_data"
IMAGE="calof/job_worker_agent:latest"
POLL_INTERVAL=5
HEARTBEAT_INTERVAL=30
STATUS_POLL_INTERVAL=10

run_cmd() {
  if $DRY_RUN; then
    echo "+ $*"
  else
    eval "$@"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--server)
      SERVER_URL="$2"; shift 2;;
    -w|--worker-id)
      WORKER_ID="$2"; shift 2;;
    -n|--nfs-server)
      NFS_SERVER="$2"; shift 2;;
    -e|--nfs-export)
      NFS_EXPORT="$2"; shift 2;;
    -m|--mount-point)
      MOUNT_POINT="$2"; shift 2;;
    -i|--image)
      IMAGE="$2"; shift 2;;
    -p|--poll)
      POLL_INTERVAL="$2"; shift 2;;
    -h|--heartbeat)
      HEARTBEAT_INTERVAL="$2"; shift 2;;
    --status-poll)
      STATUS_POLL_INTERVAL="$2"; shift 2;;
    --dry-run)
      DRY_RUN=true; shift;;
    --help)
      usage; exit 0;;
    *)
      echo "Unknown option: $1" >&2; usage; exit 1;;
  esac
done

if [[ -z "$NFS_SERVER" ]]; then
  echo "Error: --nfs-server is required" >&2
  usage; exit 1
fi

if [[ $EUID -ne 0 ]]; then
  echo "This script should be run as root (or with sudo)" >&2
  exit 1
fi

# Ensure mount point exists
run_cmd "mkdir -p '$MOUNT_POINT'"

# Mount NFS export if not already mounted
if ! mountpoint -q "$MOUNT_POINT"; then
  run_cmd "mount -t nfs ${NFS_SERVER}:${NFS_EXPORT} '$MOUNT_POINT'"
else
  echo "Mount point $MOUNT_POINT already mounted"
fi

# Pull latest image
run_cmd "docker pull '$IMAGE'"

CONTAINER_NAME="job-worker-${WORKER_ID}"

# Stop and remove existing container if needed
if ! $DRY_RUN; then
  if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    run_cmd "docker rm -f '${CONTAINER_NAME}'"
  fi
else
  echo "+ docker rm -f '${CONTAINER_NAME}' # (skipped, dry-run)"
fi

DOCKER_CMD="docker run -d --restart unless-stopped \
  --name ${CONTAINER_NAME} \
  -e OPEVA_SERVER=${SERVER_URL} \
  -e WORKER_ID=${WORKER_ID} \
  -e OPEVA_SHARED_DIR=${MOUNT_POINT} \
  -e POLL_INTERVAL=${POLL_INTERVAL} \
  -e WORKER_HEARTBEAT_INTERVAL=${HEARTBEAT_INTERVAL} \
  -e STATUS_POLL_INTERVAL=${STATUS_POLL_INTERVAL} \
  -v ${MOUNT_POINT}:${MOUNT_POINT} \
  ${IMAGE}"

run_cmd "$DOCKER_CMD"

echo "Worker ${WORKER_ID} is running. Logs: docker logs -f ${CONTAINER_NAME}"
