# Job Worker Agent

Worker implementation for the OPEVA backend. Each agent polls the API for
queued jobs, executes workloads via the configured executor (`docker` or
`deucalion`), and streams status/log updates back to the server.

## Features

- Polls `/api/agent/next-job`, `/api/agent/job-status`, and `/api/agent/heartbeat`.
- Writes logs to `jobs/<job_id>/logs/<job_id>.log` inside the shared directory.
- Backend provides the full container payload (image, command, container name,
  volumes, env) and the worker runs it as-is.
- Executor modes:
  - `docker`: runs jobs in Docker as before.
  - `deucalion`: submits jobs to Deucalion via SSH + Slurm + Singularity.
- One job at a time per agent instance—run multiple containers for parallelism.
- Heartbeat and cooperative stop support (agent stops the container when status
  becomes `stop_requested` or `canceled`).
- Periodic `job-status` updates while running to avoid stale-job handling.

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

## Deucalion mode (SSH + Slurm + Singularity)

Use the dedicated compose file when running the worker that targets Deucalion:

```bash
export WORKER_ID=deucalion
export OPEVA_SERVER=http://<backend>:8000
export LOCAL_SHARED_DIR=/mnt/opeva_shared
export OPEVA_SHARED_DIR=/mnt/opeva_shared

export DEUCALION_SSH_HOST=<login-node>
export DEUCALION_SSH_USER=<username>
export DEUCALION_SSH_KEY_PATH_HOST=/etc/opeva/deucalion/id_ed25519
export DEUCALION_SSH_KNOWN_HOSTS_HOST=/etc/opeva/deucalion/known_hosts
export DEUCALION_SIF_PATH=/projects/F202508843CPCAA0/tiagocalof/images/simulator.sif
export DEUCALION_REMOTE_ROOT=/projects/F202508843CPCAA0/tiagocalof

docker compose -f docker-compose.deucalion.yml up -d
```

Notes:
- Backend remains unchanged; this worker still uses `/api/agent/*`.
- Datasets and `.sif` are expected to already exist in Deucalion storage.
- The worker copies config + submits with `sbatch`, monitors with `squeue/sacct`,
  syncs logs periodically, and reports final status back to backend.
- If SSH is unavailable longer than `DEUCALION_UNREACHABLE_GRACE_SECONDS`,
  the worker fails the job with `error=deucalion_unreachable_timeout`.

## Laptop helper (docker compose + NFS automation)

For ad-hoc workers on a notebook that needs to mount the shared NFS directory
only while the agent runs, the `scripts/local_worker.sh` wrapper handles the
full lifecycle:

```bash
# Export overrides once per session (or source a file with these values)
export WORKER_ID=tiago-laptop
export OPEVA_SERVER=http://193.136.62.78:8000    # backend reachable via VPN/public IP
export SHUTDOWN_TIMEOUT=900                     # allow 15 minutes for graceful stop

# Mount the share (if needed) and start the worker container
sudo scripts/local_worker.sh start

# Request graceful shutdown, stop the compose stack, and unmount the share
sudo WORKER_ID=tiago-laptop scripts/local_worker.sh stop
```

Tune the behaviour by exporting variables (e.g. `NFS_SERVER`, `MOUNT_POINT`,
`WORKER_ID`, `WORKER_IMAGE`, `OPEVA_SERVER`) before running the script. The
compose definition lives in `docker-compose.local.yml`. The worker should point
to the backend using the address that is accessible from the laptop (typically
the server’s public/VPN-routed IP, e.g. `http://193.136.62.78:8000`).

If you prefer a one-liner without `export`, prefix the command:

```bash
sudo WORKER_ID=tiago-laptop OPEVA_SERVER=http://193.136.62.78:8000 SHUTDOWN_TIMEOUT=900 scripts/local_worker.sh start
```

While the worker runs you can:

- Inspect active job containers: `sudo docker ps --filter name=job_tiago-laptop`.
- Watch job logs: `sudo tail -f /mnt/opeva_shared/jobs/<job_id>/logs/<job_id>.log`.
- Check the worker state/mount: `sudo WORKER_ID=tiago-laptop scripts/local_worker.sh status`.
- logs `sudo docker logs -f job-worker-tiago-laptop`
- pull latest image `docker pull calof/job_worker_agent:latest`

Need to abort immediately? `sudo WORKER_ID=tiago-laptop scripts/local_worker.sh stop --force`
removes the worker and any job containers without waiting for the current job to finish,
and posts a `failed` status with `error="force-stop"` for each running job.
The worker always passes the job id as `--job_id <value>` to match the simulator
entrypoint and automatically requests GPUs when available, falling back to CPU
if Docker cannot satisfy the request.

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
| `WORKER_EXECUTOR` | `docker` (default) or `deucalion`. |
| `OPEVA_SHARED_DIR` | Local path to the mounted NFS share. |
| `POLL_INTERVAL` | Seconds between queue polls when idle. |
| `WORKER_HEARTBEAT_INTERVAL` | Heartbeat interval in seconds. |
| `STATUS_POLL_INTERVAL` | How often to check job status while running (seconds). |
| `LOG_LEVEL` | Python logging level (`INFO`, `DEBUG`, …). |
| `WORKER_EXIT_AFTER_JOB` | Set to `1`/`true` to stop polling after the next job finishes. |

Deucalion-only variables:

| Variable | Description |
|----------|-------------|
| `DEUCALION_SSH_HOST` / `DEUCALION_SSH_USER` | SSH endpoint for Deucalion login node. |
| `DEUCALION_SSH_PORT` | SSH port (default `22`). |
| `DEUCALION_SSH_KEY_PATH` | Path inside container to private key (recommended mount RO). |
| `DEUCALION_SSH_KNOWN_HOSTS` | Path inside container to known_hosts file (recommended mount RO). |
| `DEUCALION_REMOTE_ROOT` | Remote root directory (default `/projects/F202508843CPCAA0/tiagocalof`). |
| `DEUCALION_SIF_PATH` | Remote path to Singularity image (`.sif`) used to execute jobs. |
| `DEUCALION_POLL_INTERVAL` | Slurm state poll interval in seconds (default `10`). |
| `DEUCALION_SYNC_INTERVAL` | Remote log sync interval in seconds (default `15`). |
| `DEUCALION_UNREACHABLE_GRACE_SECONDS` | Grace window before failing unreachable jobs (default `900`). |
| `DEUCALION_SLURM_ACCOUNT_CPU/GPU` | Default Slurm accounts (`f202508843cpcaa0x` / `f202508843cpcaa0g`). |
| `DEUCALION_SLURM_PARTITION_CPU/GPU` | Default partitions (`normal-x86` / `normal-a100-80`). |
| `DEUCALION_SLURM_TIME` | Default time limit, e.g. `04:00:00`. |
| `DEUCALION_SLURM_CPUS_PER_TASK` | Default CPU cores per task. |
| `DEUCALION_SLURM_MEM_GB` | Default memory in GB. |
| `DEUCALION_SLURM_GPUS` | Default GPU count (`0` means CPU job). |

Start additional containers if you need multiple jobs running in parallel.

### Monitoring and control

- The worker writes its own logs to stdout/stderr—inspect them with
  `docker logs job-worker-<worker_id>`.
- Job payload logs are persisted to the shared directory at
  `<shared_dir>/jobs/<job_id>/logs/<job_id>.log`.
- Send `SIGUSR1` to the container (e.g. `docker kill --signal=USR1 job-worker-<id>`) to
  request a graceful shutdown after the current job completes.

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
