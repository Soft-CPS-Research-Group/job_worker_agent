#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: local_worker.sh <command>

Commands:
  start     Mount the NFS share (if needed) and start the worker via docker compose
  serve     Alias for start
  stop      Request a graceful shutdown, stop the compose stack, and unmount the share
            Use 'stop --force' to kill the worker/job containers immediately
  restart   Stop then start
  status    Show mount and container status
  logs      Follow worker logs
  mount     Mount only the NFS share
  umount    Unmount only the NFS share
  vpn       Bring up the VPN connection only
  help      Show this message

Environment overrides (export before running):
  NFS_SERVER             NFS server host/IP (default: 193.136.62.78)
  NFS_EXPORT             Export path on the server (default: /opt/opeva_shared_data)
  MOUNT_POINT            Local mount point (default: /mnt/opeva_shared)
  NFS_MOUNT_OPTS         Options passed to mount -o (default: vers=4.1,proto=tcp,port=2049)
  LOCAL_WORKER_ENV_FILE  Optional env file (default: <repo>/.local-worker.env if present)
  OPEVA_SERVER           Job Orchestrator URL (default: http://localhost:8011)
  WORKER_ID              Worker identifier (default: <hostname>-local)
  WORKER_AGENT_IMAGE     Worker agent container image (default: calof/job_worker_agent:latest)
  WORKER_JOB_IMAGE       Default job image fallback (default: calof/opeva_simulator:latest)
  WORKER_EXECUTOR        Worker executor mode (default: docker)
  WORKER_ENABLE_GPU      Enable GPU requests for docker executor (default: false)
  WORKER_REQUIRE_GPU     Fail the job if Docker cannot allocate GPU (default: false)
  WORKER_REMAP_DATA_VOLUME Remap orchestrator /data bind to local OPEVA_SHARED_DIR (default: true)
  WORKER_CONTAINER_NAME  Container name (default: job-worker-<WORKER_ID>)
  LOG_LEVEL              Logging level (default: INFO)
  POLL_INTERVAL          Queue poll interval (default: 5)
  WORKER_HEARTBEAT_INTERVAL  Heartbeat interval (default: 30)
  STATUS_POLL_INTERVAL   Status poll interval (default: 10)
  OPEVA_SHARED_DIR       Path the container sees for the share (default: MOUNT_POINT)
  SHUTDOWN_TIMEOUT       Seconds to wait for graceful stop (default: 120)
  LEAVE_MOUNT            Set to 1 to keep the share mounted after stop
  PULL_BEFORE_START      Set to 1 to docker compose pull before start (default: 0)
  VPN_CONNECTION         NetworkManager VPN connection name (default: deinet)
  VPN_REQUIRED           Set to 0 to skip VPN checks (default: 1)
  VPN_WATCHDOG           Set to 0 to disable background VPN/NFS watchdog (default: 1)
  VPN_CHECK_INTERVAL     Watchdog interval seconds (default: 30)
  VPN_CONNECT_TIMEOUT    Seconds allowed for nmcli connection up (default: 60)
  VPN_TARGET             IP used for route checks (default: NFS_SERVER)
  NFS_CHECK_TIMEOUT      Seconds allowed for watchdog NFS stat checks (default: 5)
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.local.yml"
SERVICE_NAME="worker_agent"

LOCAL_WORKER_ENV_FILE="${LOCAL_WORKER_ENV_FILE:-${REPO_ROOT}/.local-worker.env}"
if [[ -f "$LOCAL_WORKER_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$LOCAL_WORKER_ENV_FILE"
  set +a
fi

NFS_SERVER="${NFS_SERVER:-193.136.62.78}"
NFS_EXPORT="${NFS_EXPORT:-/opt/opeva_shared_data}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/opeva_shared}"
NFS_MOUNT_OPTS="${NFS_MOUNT_OPTS:-vers=4.1,proto=tcp,port=2049}"

WORKER_ID="${WORKER_ID:-$(hostname)-local}"
# Backward compatibility: old local env files used WORKER_IMAGE for the agent image.
WORKER_AGENT_IMAGE="${WORKER_AGENT_IMAGE:-${WORKER_IMAGE:-calof/job_worker_agent:latest}}"
WORKER_JOB_IMAGE="${WORKER_JOB_IMAGE:-calof/opeva_simulator:latest}"
WORKER_EXECUTOR="${WORKER_EXECUTOR:-docker}"
WORKER_ENABLE_GPU="${WORKER_ENABLE_GPU:-false}"
WORKER_REQUIRE_GPU="${WORKER_REQUIRE_GPU:-false}"
WORKER_REMAP_DATA_VOLUME="${WORKER_REMAP_DATA_VOLUME:-true}"
WORKER_CONTAINER_NAME="${WORKER_CONTAINER_NAME:-job-worker-${WORKER_ID}}"
OPEVA_SERVER="${OPEVA_SERVER:-http://localhost:8011}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"
WORKER_HEARTBEAT_INTERVAL="${WORKER_HEARTBEAT_INTERVAL:-30}"
STATUS_POLL_INTERVAL="${STATUS_POLL_INTERVAL:-10}"
OPEVA_SHARED_DIR="${OPEVA_SHARED_DIR:-${MOUNT_POINT}}"
SHUTDOWN_TIMEOUT="${SHUTDOWN_TIMEOUT:-120}"
LEAVE_MOUNT="${LEAVE_MOUNT:-0}"
PULL_BEFORE_START="${PULL_BEFORE_START:-0}"
VPN_CONNECTION="${VPN_CONNECTION:-deinet}"
VPN_REQUIRED="${VPN_REQUIRED:-1}"
VPN_WATCHDOG="${VPN_WATCHDOG:-1}"
VPN_CHECK_INTERVAL="${VPN_CHECK_INTERVAL:-30}"
VPN_CONNECT_TIMEOUT="${VPN_CONNECT_TIMEOUT:-60}"
VPN_TARGET="${VPN_TARGET:-${NFS_SERVER}}"
NFS_CHECK_TIMEOUT="${NFS_CHECK_TIMEOUT:-5}"
WATCHDOG_PID_FILE="${WATCHDOG_PID_FILE:-/tmp/opeva-${WORKER_ID}-vpn-watchdog.pid}"
WATCHDOG_LOG_FILE="${WATCHDOG_LOG_FILE:-/tmp/opeva-${WORKER_ID}-vpn-watchdog.log}"
FORCE_STOP="${FORCE_STOP:-0}"

require_root() {
  if [[ $EUID -ne 0 ]]; then
    echo "Run this script as root (e.g. sudo $0 start)" >&2
    exit 1
  fi
}

check_prereqs() {
  command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 1; }
  command -v mountpoint >/dev/null 2>&1 || { echo "mountpoint is required" >&2; exit 1; }
  command -v findmnt >/dev/null 2>&1 || { echo "findmnt is required" >&2; exit 1; }
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

container_exists() {
  docker ps -a -q --filter "name=^${WORKER_CONTAINER_NAME}$" | grep -q .
}

export_compose_env() {
export WORKER_AGENT_IMAGE
export WORKER_JOB_IMAGE
export WORKER_EXECUTOR
export WORKER_ENABLE_GPU
export WORKER_REQUIRE_GPU
export WORKER_REMAP_DATA_VOLUME
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

vpn_enabled() {
  [[ "$VPN_REQUIRED" != "0" && -n "${VPN_CONNECTION:-}" ]]
}

vpn_active() {
  vpn_enabled || return 0
  command -v nmcli >/dev/null 2>&1 || return 1
  nmcli -t -f NAME,TYPE connection show --active | grep -Fxq "${VPN_CONNECTION}:vpn"
}

prepare_vpn_connection() {
  vpn_enabled || return 0
  command -v nmcli >/dev/null 2>&1 || { echo "nmcli is required for VPN_CONNECTION=${VPN_CONNECTION}" >&2; return 1; }
  nmcli connection modify "$VPN_CONNECTION" connection.autoconnect yes connection.autoconnect-retries -1 connection.permissions "" >/dev/null 2>&1 || true
}

ensure_vpn() {
  vpn_enabled || return 0
  prepare_vpn_connection || return 1
  if vpn_active; then
    return 0
  fi
  echo "Bringing up VPN connection '${VPN_CONNECTION}'"
  if timeout "$VPN_CONNECT_TIMEOUT" nmcli connection up "$VPN_CONNECTION"; then
    return 0
  fi
  echo "Failed to bring up VPN '${VPN_CONNECTION}'. Check NetworkManager secrets/keyring for headless use." >&2
  return 1
}

route_to_target() {
  ip route get "$VPN_TARGET" 2>/dev/null | head -1 || true
}

nfs_access_ok() {
  mountpoint -q "$MOUNT_POINT" || return 1
  timeout "$NFS_CHECK_TIMEOUT" stat "$MOUNT_POINT" >/dev/null 2>&1
}

watchdog_running() {
  [[ -f "$WATCHDOG_PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$WATCHDOG_PID_FILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

start_watchdog() {
  [[ "$VPN_WATCHDOG" == "1" ]] || return 0
  if watchdog_running; then
    echo "VPN/NFS watchdog already running (pid $(cat "$WATCHDOG_PID_FILE"))"
    return 0
  fi
  echo "Starting VPN/NFS watchdog"
  nohup "$0" watchdog-loop >>"$WATCHDOG_LOG_FILE" 2>&1 &
  echo $! >"$WATCHDOG_PID_FILE"
}

stop_watchdog() {
  if ! watchdog_running; then
    rm -f "$WATCHDOG_PID_FILE"
    return 0
  fi
  local pid
  pid="$(cat "$WATCHDOG_PID_FILE")"
  echo "Stopping VPN/NFS watchdog (pid ${pid})"
  kill "$pid" >/dev/null 2>&1 || true
  rm -f "$WATCHDOG_PID_FILE"
}

watchdog_loop() {
  require_root
  check_prereqs
  echo "[$(date --iso-8601=seconds)] watchdog started for ${WORKER_ID}"
  while true; do
    if ! ensure_vpn; then
      echo "[$(date --iso-8601=seconds)] VPN check failed"
    fi
    if ! mountpoint -q "$MOUNT_POINT"; then
      echo "[$(date --iso-8601=seconds)] NFS not mounted; attempting mount"
      mount_share || echo "[$(date --iso-8601=seconds)] NFS mount failed"
    elif ! nfs_access_ok; then
      echo "[$(date --iso-8601=seconds)] NFS mounted but not responding; route: $(route_to_target)"
    fi
    sleep "$VPN_CHECK_INTERVAL"
  done
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

job_containers_for_worker() {
  {
    docker ps -aq --filter "label=opeva.worker_id=${WORKER_ID}" 2>/dev/null || true
    docker ps -aq --filter "name=^job_${WORKER_ID}_" 2>/dev/null || true
  } | awk 'NF && !seen[$0]++'
}

force_remove_jobs() {
  local containers=()
  mapfile -t containers < <(job_containers_for_worker)
  if [[ ${#containers[@]} -eq 0 ]]; then
    return 0
  fi
  echo "Force removing job containers: ${containers[*]}"
  local cid job_id
  for cid in "${containers[@]}"; do
    job_id=$(docker inspect --format '{{ index .Config.Labels "opeva.job_id" }}' "$cid" 2>/dev/null || true)
    if [[ -n "$job_id" && "$job_id" != "<no value>" ]]; then
      post_force_status "$job_id"
    fi
  done
  docker rm -f "${containers[@]}" >/dev/null 2>&1 || true
}

start_worker() {
  require_root
  check_prereqs
  ensure_vpn
  mount_share
  export_compose_env
  if container_running; then
    echo "Worker container ${WORKER_CONTAINER_NAME} already running"
    start_watchdog
    return 0
  fi
  if [[ "$PULL_BEFORE_START" == "1" ]]; then
    echo "Pulling worker image"
    compose_cmd pull "$SERVICE_NAME"
  fi
  echo "Starting worker via docker compose"
  compose_cmd up -d "$SERVICE_NAME"
  start_watchdog
  echo "Worker started. Follow logs with: sudo ${0} logs"
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
  if ! container_exists; then
    echo "Worker container ${WORKER_CONTAINER_NAME} is not present"
  else
    if [[ "${FORCE_STOP}" == "1" ]]; then
      echo "Force removing worker container ${WORKER_CONTAINER_NAME}"
      docker rm -f "${WORKER_CONTAINER_NAME}" >/dev/null 2>&1 || true
      force_remove_jobs
    elif container_running; then
      echo "Requesting graceful shutdown (SIGUSR1)"
      docker update --restart=no "${WORKER_CONTAINER_NAME}" >/dev/null 2>&1 || true
      docker kill --signal=USR1 "${WORKER_CONTAINER_NAME}" >/dev/null
      if ! wait_for_stop; then
        echo "Graceful shutdown is still pending; leaving worker running."
        echo "Re-run this command to keep waiting, increase SHUTDOWN_TIMEOUT, or use 'stop --force' to abort."
        return 1
      fi
    else
      echo "Worker container already stopped"
    fi
  fi
  echo "Bringing down compose stack"
  compose_cmd down --remove-orphans
  stop_watchdog
  if [[ "$LEAVE_MOUNT" != "1" ]]; then
    unmount_share
  else
    echo "Leaving ${MOUNT_POINT} mounted (LEAVE_MOUNT=1)"
  fi
  FORCE_STOP=0
}

status_worker() {
  check_prereqs
  echo "Worker ID: ${WORKER_ID}"
  echo "Orchestrator: ${OPEVA_SERVER}"
  echo "Agent image: ${WORKER_AGENT_IMAGE}"
  echo "Default job image: ${WORKER_JOB_IMAGE}"
  echo "Mount point: ${MOUNT_POINT}"
  if mountpoint -q "$MOUNT_POINT"; then
    echo "  mounted from $(findmnt_source)"
  else
    echo "  not mounted"
  fi
  if vpn_enabled; then
    if vpn_active; then
      echo "VPN ${VPN_CONNECTION}: active"
    else
      echo "VPN ${VPN_CONNECTION}: inactive"
    fi
    echo "Route to ${VPN_TARGET}: $(route_to_target)"
  fi
  if watchdog_running; then
    echo "VPN/NFS watchdog: running (pid $(cat "$WATCHDOG_PID_FILE"))"
  else
    echo "VPN/NFS watchdog: stopped"
  fi
  if mountpoint -q "$MOUNT_POINT"; then
    if nfs_access_ok; then
      echo "NFS access: ok"
    else
      echo "NFS access: not responding"
    fi
  fi
  if docker ps -a -q --filter "name=^${WORKER_CONTAINER_NAME}$" | grep -q .; then
    local state
    state="$(docker inspect --format '{{.State.Status}}' "${WORKER_CONTAINER_NAME}")"
    echo "Container ${WORKER_CONTAINER_NAME}: ${state}"
  else
    echo "Container ${WORKER_CONTAINER_NAME}: not created"
  fi
}

logs_worker() {
  check_prereqs
  docker logs -f "${WORKER_CONTAINER_NAME}"
}

main() {
  local cmd="help"
  if [[ $# -gt 0 ]]; then
    cmd="$1"
    shift
  fi
  case "$cmd" in
    start|serve) start_worker ;;
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
    logs) logs_worker ;;
    mount)
      require_root
      check_prereqs
      ensure_vpn
      mount_share
      ;;
    umount|unmount)
      require_root
      check_prereqs
      stop_watchdog
      unmount_share
      ;;
    vpn)
      require_root
      check_prereqs
      ensure_vpn
      ;;
    watchdog-loop) watchdog_loop ;;
    help|--help|-h) usage ;;
    *)
      echo "Unknown command: $cmd" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
