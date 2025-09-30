# Job Worker Agent

Worker implementation for the OPEVA backend. Each agent polls the API for
queued jobs, runs the Docker workload with the shared NFS volume mounted, and
streams status/log updates back to the server.

## Features

- Polls `/api/agent/next-job`, `/api/agent/job-status`, and `/api/agent/heartbeat`.
- Writes logs to `jobs/<job_id>/logs/<job_id>.log` inside the shared directory.
- Backend provides only `job_id`, `job_name`, and `config_path`; the worker owns
  the Docker image/command.
- One job at a time per agent instance—run multiple containers for parallelism.
- Heartbeat and cooperative cancellation support (agent stops the container when
  status becomes `stopped`/`canceled`).

## Quick start (recommended)

1. Install Docker and the NFS client (`nfs-common` on Debian/Ubuntu).
2. Run the helper script as root—it mounts the NFS export and launches the
   container:

```bash
sudo scripts/setup_worker.sh \
  --server http://backend:8000 \
  --worker-id worker-a \
  --nfs-server 10.0.0.5 \
  --nfs-export /opt/opeva_shared_data
```

By default the script uses the published image `calof/job_worker_agent:latest`
and names the container `job-worker-<worker_id>`.

## Manual setup

### 1. Mount the NFS share

```bash
sudo apt install -y nfs-common
sudo mkdir -p /opt/opeva_shared_data
sudo mount -t nfs 10.0.0.5:/opt/opeva_shared_data /opt/opeva_shared_data
```

Adjust the server/export paths as needed. Add an `/etc/fstab` entry if you want
it to persist across reboots.

### 2. Run the worker container

```bash
docker run -d --restart unless-stopped \
  --name job-worker-worker-a \
  -e OPEVA_SERVER=http://backend:8000 \
  -e WORKER_ID=worker-a \
  -e OPEVA_SHARED_DIR=/opt/opeva_shared_data \
  -e POLL_INTERVAL=5 \
  -e WORKER_HEARTBEAT_INTERVAL=30 \
  -e STATUS_POLL_INTERVAL=10 \
  -v /opt/opeva_shared_data:/opt/opeva_shared_data \
  calof/job_worker_agent:latest
```

Environment variables:

| Variable | Description |
|----------|-------------|
| `OPEVA_SERVER` | Backend base URL (default `http://localhost:8000`). |
| `WORKER_ID` | Worker identifier; defaults to container hostname. |
| `OPEVA_SHARED_DIR` | Local path to the mounted NFS share. |
| `POLL_INTERVAL` | Seconds between queue polls when idle. |
| `WORKER_HEARTBEAT_INTERVAL` | Heartbeat interval in seconds. |
| `STATUS_POLL_INTERVAL` | How often to check job status while running (seconds). |
| `LOG_LEVEL` | Python logging level (`INFO`, `DEBUG`, …). |

Start additional containers if you need multiple jobs running in parallel.

## Tests (optional)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[test]
# or: pip install -r requirements-dev.txt
pytest
```

## Continuous integration & image publishing

`.github/workflows/ci.yml` runs tests and pushes
`calof/job_worker_agent:<sha>` (and `:latest` on `main`). Configure the secrets
`DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` in GitHub to enable the push step.
