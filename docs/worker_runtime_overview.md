# Worker Runtime Overview

This document summarizes how the worker agent operates so that backend
implementations can provide the correct contracts. The intent is to keep this
as the single reference when coordinating changes between the backend and the
worker service.

## High-level flow

1. **Heartbeat:** the worker periodically calls `POST /api/agent/heartbeat` with
   `{"worker_id": <id>, "info": {...}}`. A heartbeat is always sent immediately
   after a job finishes, even if the configured interval is `0`.
2. **Poll for work:** the worker calls `POST /api/agent/next-job` with
   `{"worker_id": <id>}`.
   - `204 No Content` means no work is available.
   - Any other non-2xx code is treated as an error.
   - A successful response returns JSON describing the job (see below).
3. **Run the job via executor:**
   - `docker` executor: launches the configured image with the shared directory
     mounted at `/data`.
   - `deucalion` executor: submits an Slurm job via SSH (`sbatch`) and runs the
     payload command through Singularity (`.sif`) on Deucalion (`run` by default,
     optional `exec` override).
4. **Stream/sync logs:** logs are persisted at
   `<shared_dir>/jobs/<job_id>/logs/<job_id>.log` (container stream in docker
   mode, incremental remote sync in deucalion mode).
5. **Status updates:**
   - The worker posts `POST /api/agent/job-status` with
     `{"job_id": <id>, "worker_id": <id>, "status": <status>, ...}`.
   - While running, it sends periodic `status="running"` updates to refresh
     `status_updated_at` on the server (avoids stale-job handling).
   - Terminal transitions: `finished`, `failed`, `stopped`, or `canceled`.
   - The worker includes `container_id`, `container_name`, and `exit_code` when
     available.
6. **Cooperative stop/cancel:** when configured with a positive
   `status_poll_interval`, the worker polls `GET /status/<job_id>`.
   - If the response JSON contains `{"status": "stop_requested"}` the
     worker stops the container and reports `status="stopped"`.
   - If the response JSON contains `{"status": "canceled"}` the
     worker stops the container and reports `status="canceled"`.
7. **Graceful shutdown:** operators can set `WORKER_EXIT_AFTER_JOB=1` (or pass
   `--exit-after-job`) to terminate after the next job finishes. At runtime a
   `SIGUSR1` signal triggers the same behaviour.

Worker logs (agent control flow) go to stdout/stderr, while job stdout/stderr is
persisted to `<shared_dir>/jobs/<job_id>/logs/<job_id>.log`.

## Job payload contract

Responses from `/api/agent/next-job` must include:

- `job_id` *(str, required)* – unique identifier for the job.
- `config_path` *(str, required)* – path within the shared directory that the
  container should read (`/data/<config_path>` inside the container).
- `job_name` *(str, optional)* – human-friendly name used for log readability.
- `image` *(str, optional)* – container image; falls back to worker default.
- `command` *(str, optional)* – container command; falls back to
  `--config /data/<config_path> --job_id <job_id>`.
- `container_name` *(str, optional)* – explicit name for the container.
- `volumes` *(list, optional)* – explicit volume mappings.
- `env` *(dict, optional)* – environment variables to pass to the container.

Extra fields are ignored by the worker but are preserved when reporting status
updates.

## Docker execution details

- **Image:** provided by payload or configured per worker via CLI flags
  (default `calof/opeva_simulator:latest`).
- **Command:** provided by payload or `--config /data/<config_path> --job_id <job_id>`.
- **Volume:** provided by payload or the shared directory is mounted read/write to `/data`.
- **GPU support:** enable with `WORKER_ENABLE_GPU=true` on GPU-capable hosts.
  When enabled, the worker retries without GPUs if allocation fails.
- **Container name:** `job_<worker_id>_<job_name_forced_snake_case>_<job_id[:8]>`.
- **Labels:** `opeva.worker_id` and `opeva.job_id` are attached to each job container.
- **Log file:** `<shared_dir>/jobs/<job_id>/logs/<job_id>.log` (directories are
  created automatically).

## Deucalion execution details

- The worker connects to Deucalion over SSH and validates remote prerequisites:
  - remote root exists (default `/projects/F202508843CPCAA0/tiagocalof`)
  - configured `.sif` exists
  - optional `execution.deucalion.required_paths` exist
- The worker writes config + sbatch script under:
  - `<remote_root>/runs/<job_id>/`
- If `execution.deucalion.datasets` is provided, worker ensures each dataset
  exists on Deucalion (`<remote_root>/<relative_path>`) using copy-if-missing.
- Worker creates a per-job symlink:
  - `<remote_root>/runs/<job_id>/data/datasets -> <remote_root>/datasets`
  so configs using `/data/datasets/...` keep working.
- Slurm lifecycle:
  - submit: `sbatch --parsable`
  - monitor active states: `squeue`
  - resolve terminal state/exit code: `sacct` (prefers root job row over steps)
  - cooperative stop/cancel: `scancel`
- Status mapping to backend:
  - queued/running Slurm states -> `dispatched` / `running`
  - completed exit 0 -> `finished`
  - cancellation by backend -> `stopped` or `canceled`
  - other terminal states -> `failed`
- Connectivity policy:
  - heartbeat continues independently from SSH availability
  - if SSH remains unavailable beyond
    `DEUCALION_UNREACHABLE_GRACE_SECONDS`, job is marked `failed` with
    `error=deucalion_unreachable_timeout`
- UNKNOWN policy:
  - transient `UNKNOWN` keeps monitoring
  - continuous `UNKNOWN` longer than
    `DEUCALION_UNKNOWN_STATE_TIMEOUT_SECONDS` fails with
    `error=slurm_unknown_timeout`
- Job-specific overrides can be defined in YAML under `execution.deucalion.*`
  (account/partition/time/cpus/mem/gpus/modules/sif_path/required_paths/
  command_mode/datasets).

## CLI & configuration inputs

The CLI front-end (`worker_agent.cli:main`) accepts the following arguments and
reads matching environment variables:

| CLI flag | Environment variable | Default |
| --- | --- | --- |
| `--server` | `OPEVA_SERVER` | `http://localhost:8000` |
| `--worker-id` | `WORKER_ID` | hostname at runtime |
| `--shared-dir` | `OPEVA_SHARED_DIR` | `/opt/opeva_shared_data` |
| `--image` | `WORKER_IMAGE` | `calof/opeva_simulator:latest` |
| `--executor` | `WORKER_EXECUTOR` | `docker` |
| `--poll-interval` | `POLL_INTERVAL` | `5` seconds |
| `--heartbeat-interval` | `WORKER_HEARTBEAT_INTERVAL` | `30` seconds |
| `--status-poll-interval` | `STATUS_POLL_INTERVAL` | `10` seconds |
| `--exit-after-job` | `WORKER_EXIT_AFTER_JOB` | `False` |
| `--log-level` | `LOG_LEVEL` | `INFO` |

Setting `--status-poll-interval 0` disables cooperative cancellation checks but
all other reporting remains intact.

Deucalion mode requires SSH settings and Slurm defaults via environment:
`DEUCALION_SSH_*`, `DEUCALION_SIF_PATH`, optional
`DEUCALION_SIF_COMMAND_MODE`, `DEUCALION_UNKNOWN_STATE_TIMEOUT_SECONDS`, and
optional `DEUCALION_SLURM_*`.

## Backend expectations

- `/api/agent/heartbeat` and `/api/agent/job-status` should be idempotent.
- `/api/agent/next-job` must respond quickly; long-polling should either be
  implemented on the backend or via short poll intervals on the worker.
- `/status/<job_id>` should return `404` when the job is unknown and include a
  JSON object with at least a `status` field when known.
- Backend should ensure that the referenced config files exist on the shared
  filesystem prior to assigning a job.

### Heartbeat `info` payload (current contract)

Every heartbeat includes:

- `executor` – `docker` or `deucalion`
- `worker_version` – package/app version string
- `active_job_id` – currently active job id (or `null`)
- `active_job_count` – number of active jobs on this worker (currently `0` or `1`)
- `last_job_id` – latest job seen by this worker
- `last_terminal_status` – last terminal status emitted by this worker

Deucalion workers additionally include:

- `budget` – parsed snapshot from `billing` output (`accounts`, used/limit/percent)
- `budget_refreshed_at` – UNIX timestamp for last successful budget refresh

Budget refresh is cached and executed at most once per
`DEUCALION_BUDGET_REFRESH_INTERVAL_SECONDS` (default `3600`).

Keeping this contract stable allows the worker agent and backend to evolve
independently while preserving compatibility.
