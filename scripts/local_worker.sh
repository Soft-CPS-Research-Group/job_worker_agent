#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: local_worker.sh <command>

Commands:
  start     Mount the NFS share (if needed) and start the worker via docker compose
  stop      Request a graceful shutdown, stop the compose stack, and unmount the share
            Use 'stop --force' to kill the worker/job containers immediately
  restart   Stop then start
  status    Show mount and container status
  help      Show this message

Environment overrides (export before running):
  NFS_SERVER             NFS server host/IP (default: softcps)
  NFS_EXPORT             Export path on the server (default: /opt/opeva_shared_data)
  MOUNT_POINT            Local mount point (default: /mnt/opeva_shared)
  NFS_MOUNT_OPTS         Options passed to mount -o (default: vers=4.1,proto=tcp,port=2049)
  OPEVA_SERVER           Backend URL (default: http://localhost:8000)
  WORKER_ID              Worker identifier (default: <hostname>-local)
  WORKER_IMAGE           Worker container image (default: calof/job_worker_agent:latest)
  WORKER_CONTAINER_NAME  Container name (default: job-worker-<WORKER_ID>)
  LOG_LEVEL              Logging level (default: INFO)
  POLL_INTERVAL          Queue poll interval (default: 5)
  WORKER_HEARTBEAT_INTERVAL  Heartbeat interval (default: 30)
  STATUS_POLL_INTERVAL   Status poll interval (default: 10)
  OPEVA_SHARED_DIR       Path the container sees for the share (default: MOUNT_POINT)
  SHUTDOWN_TIMEOUT       Seconds to wait for graceful stop (default: 120)
  LEAVE_MOUNT            Set to 1 to keep the share mounted after stop
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.local.yml"
SERVICE_NAME="worker_agent"

NFS_SERVER="${NFS_SERVER:-softcps}"
NFS_EXPORT="${NFS_EXPORT:-/opt/opeva_shared_data}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/opeva_shared}"
NFS_MOUNT_OPTS="${NFS_MOUNT_OPTS:-vers=4.1,proto=tcp,port=2049}"

WORKER_ID="${WORKER_ID:-$(hostname)-local}"
WORKER_IMAGE="${WORKER_IMAGE:-calof/job_worker_agent:latest}"
WORKER_CONTAINER_NAME="${WORKER_CONTAINER_NAME:-job-worker-${WORKER_ID}}"
OPEVA_SERVER="${OPEVA_SERVER:-http://localhost:8000}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"
WORKER_HEARTBEAT_INTERVAL="${WORKER_HEARTBEAT_INTERVAL:-30}"
STATUS_POLL_INTERVAL="${STATUS_POLL_INTERVAL:-10}"
OPEVA_SHARED_DIR="${OPEVA_SHARED_DIR:-${MOUNT_POINT}}"
SHUTDOWN_TIMEOUT="${SHUTDOWN_TIMEOUT:-120}"
LEAVE_MOUNT="${LEAVE_MOUNT:-0}"
FORCE_STOP="${FORCE_STOP:-0}"

require_root() {
  if [[ $EUID -ne 0 ]]; then
    echo "Run this script as root (e.g. sudo $0 start)" >&2
    exit 1
  fi
}

check_prereqs() {
  command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 1; }
  if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose plugin is required" >&2
    exit 1
  fi
  if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "Compose file not found at ${COMPOSE_FILE}" >&2
    exit 1
  fi
}

mount_share() {
  mkdir -p "$MOUNT_POINT"
  if mountpoint -q "$MOUNT_POINT"; then
    echo "Mount point ${MOUNT_POINT} already mounted"
    return 0
  fi
  echo "Mounting ${NFS_SERVER}:${NFS_EXPORT} -> ${MOUNT_POINT}"
  mount -t nfs4 -o "$NFS_MOUNT_OPTS" "${NFS_SERVER}:${NFS_EXPORT}" "$MOUNT_POINT"
}

findmnt_source() {
  findmnt -rn -o SOURCE --target "$MOUNT_POINT" 2>/dev/null || true
}

unmount_share() {
  if ! mountpoint -q "$MOUNT_POINT"; then
    return 0
  fi
  local current_source
  current_source="$(findmnt_source)"
  if [[ "$current_source" != "${NFS_SERVER}:${NFS_EXPORT}"* ]]; then
    echo "Skipping unmount of ${MOUNT_POINT} (currently ${current_source})"
    return 0
  fi
  echo "Unmounting ${MOUNT_POINT}"
  umount "$MOUNT_POINT"
}

container_running() {
  docker ps -q --filter "name=^${WORKER_CONTAINER_NAME}$" | grep -q .
}

export_compose_env() {
export WORKER_IMAGE
export WORKER_CONTAINER_NAME
export OPEVA_SERVER
export WORKER_ID
export OPEVA_SHARED_DIR
export LOG_LEVEL
export POLL_INTERVAL
export WORKER_HEARTBEAT_INTERVAL
export STATUS_POLL_INTERVAL
export LOCAL_SHARED_DIR="$MOUNT_POINT"
}

compose_cmd() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

post_force_status() {
  local job_id="$1"
  if [[ -z "${OPEVA_SERVER:-}" ]]; then
    return
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl not available; cannot report force-stop for job ${job_id}" >&2
    return
  fi
  local payload
  payload=$(printf '{"job_id":"%s","worker_id":"%s","status":"failed","exit_code":137,"error":"force-stop"}' "$job_id" "$WORKER_ID")
  local url="${OPEVA_SERVER%/}/api/agent/job-status"
  curl -sS -X POST "$url" -H "Content-Type: application/json" -d "$payload" >/dev/null 2>&1 || \
    echo "Warning: failed to report force-stop for job ${job_id}" >&2
}

start_worker() {
  require_root
  check_prereqs
  mount_share
  export_compose_env
  if container_running; then
    echo "Worker container ${WORKER_CONTAINER_NAME} already running"
    return 0
  fi
  echo "Starting worker via docker compose"
  compose_cmd up -d "$SERVICE_NAME"
  echo "Worker started. Follow logs with: docker logs -f ${WORKER_CONTAINER_NAME}"
}

wait_for_stop() {
  local deadline=$(( $(date +%s) + SHUTDOWN_TIMEOUT ))
  while container_running; do
    if [[ $(date +%s) -ge $deadline ]]; then
      echo "Timed out waiting for graceful shutdown after ${SHUTDOWN_TIMEOUT}s"
      return 1
    fi
    sleep 2
  done
  return 0
}

stop_worker() {
  require_root
  check_prereqs
  export_compose_env
  local job_containers=()
  if ! docker ps -a -q --filter "name=^${WORKER_CONTAINER_NAME}$" | grep -q .; then
    echo "Worker container ${WORKER_CONTAINER_NAME} is not present"
  else
    if [[ "${FORCE_STOP}" == "1" ]]; then
      echo "Force removing worker container ${WORKER_CONTAINER_NAME}"
      docker rm -f "${WORKER_CONTAINER_NAME}" >/dev/null 2>&1 || true
      mapfile -t job_containers < <(docker ps -aq --filter "name=^job_${WORKER_ID}_")
      if [[ ${#job_containers[@]} -gt 0 ]]; then
        echo "Force removing job containers: ${job_containers[*]}"
        for cid in "${job_containers[@]}"; do
          local job_id
          job_id=$(docker inspect --format '{{ index .Config.Labels "opeva.job_id" }}' "$cid" 2>/dev/null || true)
          if [[ -n "$job_id" && "$job_id" != "<no value>" ]]; then
            post_force_status "$job_id"
          fi
        done
        docker rm -f "${job_containers[@]}" >/dev/null 2>&1 || true
      fi
    elif container_running; then
      echo "Requesting graceful shutdown (SIGUSR1)"
      docker kill --signal=USR1 "${WORKER_CONTAINER_NAME}" >/dev/null
      if ! wait_for_stop; then
        echo "Forcing container stop"
      fi
    else
      echo "Worker container already stopped"
    fi
  fi
  echo "Bringing down compose stack"
  compose_cmd down --remove-orphans
  if [[ "$LEAVE_MOUNT" != "1" ]]; then
    unmount_share
  else
    echo "Leaving ${MOUNT_POINT} mounted (LEAVE_MOUNT=1)"
  fi
  FORCE_STOP=0
}

status_worker() {
  check_prereqs
  echo "Mount point: ${MOUNT_POINT}"
  if mountpoint -q "$MOUNT_POINT"; then
    echo "  mounted from $(findmnt_source)"
  else
    echo "  not mounted"
  fi
  if docker ps -a -q --filter "name=^${WORKER_CONTAINER_NAME}$" | grep -q .; then
    local state
    state="$(docker inspect --format '{{.State.Status}}' "${WORKER_CONTAINER_NAME}")"
    echo "Container ${WORKER_CONTAINER_NAME}: ${state}"
  else
    echo "Container ${WORKER_CONTAINER_NAME}: not created"
  fi
}

main() {
  local cmd="help"
  if [[ $# -gt 0 ]]; then
    cmd="$1"
    shift
  fi
  case "$cmd" in
    start) start_worker ;;
    stop)
      if [[ "${1:-}" == "--force" || "${1:-}" == "-f" ]]; then
        FORCE_STOP=1
        shift || true
      fi
      stop_worker
      ;;
    restart)
      stop_worker
      start_worker
      ;;
    status) status_worker ;;
    help|--help|-h) usage ;;
    *)
      echo "Unknown command: $cmd" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
