# Worker Runtime Overview

This document summarizes how the worker agent operates so that backend
implementations can provide the correct contracts. The intent is to keep this
as the single reference when coordinating changes between the backend and the
worker service.

## High-level flow

1. **Heartbeat:** the worker periodically calls `POST /api/agent/heartbeat` with
   `{"worker_id": <id>}`. A heartbeat is always sent immediately after a job
   finishes, even if the configured interval is `0`.
2. **Poll for work:** the worker calls `POST /api/agent/next-job` with
   `{"worker_id": <id>}`.
   - `204 No Content` means no work is available.
   - Any other non-2xx code is treated as an error.
   - A successful response returns JSON describing the job (see below).
3. **Run the job inside Docker:** the worker launches the configured image with
   the shared directory mounted at `/data` and a command built from the job
   payload.
4. **Stream logs:** container stdout/stderr is appended to
   `<shared_dir>/jobs/<job_id>/logs/<job_id>.log`.
5. **Status updates:**
   - The worker posts `POST /api/agent/job-status` with
     `{"job_id": <id>, "worker_id": <id>, "status": <status>, ...}`.
   - Status transitions follow: `running` → `finished|failed|stopped|canceled`.
   - The worker includes `container_id`, `container_name`, and `exit_code` when
     available.
6. **Cooperative cancellation:** when configured with a positive
   `status_poll_interval`, the worker polls `GET /status/<job_id>`.
   - If the response JSON contains `{"status": "stopped"|"canceled"}` the
     worker stops the container and reports that status to the backend.

## Job payload contract

Responses from `/api/agent/next-job` must include:

- `job_id` *(str, required)* – unique identifier for the job.
- `config_path` *(str, required)* – path within the shared directory that the
  container should read (`/data/<config_path>` inside the container).
- `job_name` *(str, optional)* – human-friendly name used for log readability
  and container naming (defaults to `job_id` when missing).

Extra fields are ignored by the worker but are preserved when reporting status
updates.

## Container execution details

- **Image:** configured per worker via CLI flags or environment variables
  (default `calof/opeva_simulator:latest`).
- **Command:** `--config /data/<config_path> --job_id <job_id>`.
- **Volume:** the shared directory is mounted read/write to `/data`.
- **Container name:** `job_<worker_id>_<job_name_forced_snake_case>_<job_id[:8]>`.
- **Log file:** `<shared_dir>/jobs/<job_id>/logs/<job_id>.log` (directories are
  created automatically).

## CLI & configuration inputs

The CLI front-end (`worker_agent.cli:main`) accepts the following arguments and
reads matching environment variables:

| CLI flag | Environment variable | Default |
| --- | --- | --- |
| `--server` | `OPEVA_SERVER` | `http://localhost:8000` |
| `--worker-id` | `WORKER_ID` | hostname at runtime |
| `--shared-dir` | `OPEVA_SHARED_DIR` | `/opt/opeva_shared_data` |
| `--image` | `WORKER_IMAGE` | `calof/opeva_simulator:latest` |
| `--poll-interval` | `POLL_INTERVAL` | `5` seconds |
| `--heartbeat-interval` | `WORKER_HEARTBEAT_INTERVAL` | `30` seconds |
| `--status-poll-interval` | `STATUS_POLL_INTERVAL` | `10` seconds |
| `--log-level` | `LOG_LEVEL` | `INFO` |

Setting `--status-poll-interval 0` disables cooperative cancellation checks but
all other reporting remains intact.

## Backend expectations

- `/api/agent/heartbeat` and `/api/agent/job-status` should be idempotent.
- `/api/agent/next-job` must respond quickly; long-polling should either be
  implemented on the backend or via short poll intervals on the worker.
- `/status/<job_id>` should return `404` when the job is unknown and include a
  JSON object with at least a `status` field when known.
- Backend should ensure that the referenced config files exist on the shared
  filesystem prior to assigning a job.

Keeping this contract stable allows the worker agent and backend to evolve
independently while preserving compatibility.
