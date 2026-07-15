# Job Worker Agent

Worker implementation for the OPEVA Job Orchestrator. Each agent polls the API for
queued jobs, executes workloads via the configured executor (`docker`,
`deucalion` or `union`), and streams status/log updates back to the server.

## Features

- Polls `/api/agent/next-job`, `/api/agent/job-status`, and `/api/agent/heartbeat`.
- Writes logs to `jobs/<job_id>/logs/<job_id>.log` inside the shared directory.
- Job Orchestrator provides the full container payload (image, command, container name,
  volumes, env) and the worker runs it as-is.
- Executor modes:
  - `docker`: runs jobs in Docker as before.
  - `deucalion`: submits jobs to Deucalion via SSH + Slurm + Singularity.
  - `union`: submits GPU jobs to Union INESC TEC using a runner and Algorithms sidecar.
- Configurable concurrent slots per worker, with one slot as the default.
- Heartbeat and cooperative stop support (agent stops the container when status
  becomes `stop_requested` or `canceled`).
- Periodic `job-status` updates while running to avoid stale-job handling.
- Negotiates `attempt_fencing_v1` with compatible orchestrators and echoes the
  opaque dispatch token on every status update. This prevents a late status
  from an old execution changing a requeued job. A stale-attempt rejection also
  terminates the superseded Docker container, Slurm job or Union Run. Tokens
  are redacted from logs.

Current package version: `0.5.0`. Release notes live in [`docs/releases.md`](docs/releases.md).

## Union INESC TEC mode

The Union bridge uses the dedicated `Dockerfile.union` image. It packages the
resolved config and referenced datasets, uploads them to Union object storage,
and launches a two-container task: the worker runner as primary and the
selected Algorithms image as a one-GPU sidecar. Logs, progress and final
artifacts are synchronized back into the existing shared job directory.

Required configuration includes:

```bash
WORKER_ID=union-inesctec
WORKER_EXECUTOR=union
WORKER_MAX_ACTIVE_JOBS=10
UNION_AUTH_MODE=device_flow
UNION_OBJECT_STORE_CA_FILE=/run/secrets/union_object_store_ca.pem
UNION_ENDPOINT=dns:///inesctec.hosted.unionai.cloud
UNION_ORG=inesctec
UNION_PROJECT=humanise-energaize
UNION_DOMAIN=development
UNION_RUNNER_IMAGE=calof/job_worker_agent:union-latest
UNION_GPU_COUNT=1
UNION_UNREACHABLE_GRACE_SECONDS=900
UNION_RETRY_MAX_BACKOFF_SECONDS=60
```

`UNION_OBJECT_STORE_CA_FILE` is used only for S3 artifact traffic. Set the
separate optional `UNION_CONTROL_PLANE_CA_FILE` only if the Union API endpoint
itself uses a private CA.

Runtime state is persisted atomically at
`jobs/<job_id>/.worker/union.json`. Restarting the bridge recovers an existing
run by deterministic job/attempt ID, including a restart during upload or
submission. Temporary Union control-plane failures use bounded exponential
backoff and keep the job in `setup` or `running`; they do not create a second
run. Result installation, remote cleanup and final orchestrator acknowledgment
are persisted as separate idempotent steps.
Union recovery state retains the dispatch attempt fields in its mode-`0600`
state file so terminal delivery remains fenced after a bridge restart.
In `device_flow` mode the Flyte keyring must be mounted from a persistent Docker
volume. The worker refreshes stored credentials automatically and advertises a
browser URL/code in heartbeat telemetry only when user authentication is needed.
The same hook covers refresh failure during run monitoring: the remote run is
left intact, UI authentication is requested, and the blocked reconciliation
call resumes after login.
API-key mode remains available by setting `UNION_AUTH_MODE=api_key` and mounting
`FLYTE_API_KEY_FILE`. Secrets and CA files must not be baked into either image.

## Quick start (recommended)

1. Install Docker and the NFS client (`nfs-common` on Debian/Ubuntu).
2. Run the helper script as root—it mounts the NFS export and launches the
   container:

```bash
sudo scripts/setup_worker.sh \
  --server http://job_orchestrator_agent:8011 \
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
export OPEVA_SERVER=http://<orchestrator>:8011
export LOCAL_SHARED_DIR=/mnt/opeva_shared
export OPEVA_SHARED_DIR=/mnt/opeva_shared

export DEUCALION_SSH_HOST=<login-node>
export DEUCALION_SSH_USER=<username>
export DEUCALION_SSH_KEY_PATH_HOST=/etc/opeva/deucalion/id_ed25519
export DEUCALION_SSH_KNOWN_HOSTS_HOST=/etc/opeva/deucalion/known_hosts
export DEUCALION_SIF_REPOSITORY=calof/opeva_simulator_sif
export DEUCALION_REMOTE_ROOT=/projects/F202508843CPCAA0/tiagocalof

docker compose -f docker-compose.deucalion.yml up -d
```

Notes:
- The worker talks to the Job Orchestrator using `/api/agent/*`.
- Worker resolves SIF by job image tag from OCI artifacts (default repo: `calof/opeva_simulator_sif`)
  and stores them in a versioned cache under Deucalion remote storage.
- Datasets can be synchronized automatically per job using
  `execution.deucalion.datasets` (paths relative to the shared root, e.g.
  `datasets/site_a/input.csv`).
- The worker copies config + submits with `sbatch`, monitors with `squeue/sacct`,
  syncs logs incrementally, and reports final status back to the orchestrator.
- Deucalion Slurm profiles are validated before `sbatch`: dev partitions are
  capped at 4h, normal partitions at 48h, and large CPU/ARM partitions at 72h.
- Artifact sync prioritizes the current simulator layout under
  `<remote_root>/runs/<job_id>/data/jobs/<job_id>/(results|progress)` and falls
  back to legacy `<remote_root>/runs/<job_id>/(results|progress)` if needed.
- If SSH is unavailable longer than `DEUCALION_UNREACHABLE_GRACE_SECONDS`,
  the worker fails the job with `error=deucalion_unreachable_timeout`.

## Laptop helper (docker compose + NFS automation)

For ad-hoc workers on a notebook that needs to mount the shared NFS directory
only while the agent runs, the `scripts/local_worker.sh` wrapper handles the
full lifecycle:

Quick command notes live in [`docs/laptop_worker_notes.md`](docs/laptop_worker_notes.md).

```bash
# Export overrides once per session (or put them in .local-worker.env)
export WORKER_ID=tiago-laptop
export OPEVA_SERVER=http://193.136.62.78:8011    # orchestrator reachable via VPN/public IP
export VPN_CONNECTION=deinet                    # NetworkManager VPN profile
export WORKER_ENABLE_GPU=true
export WORKER_REQUIRE_GPU=true                  # fail instead of silently falling back to CPU
export SHUTDOWN_TIMEOUT=900                     # allow 15 minutes for graceful stop

# Bring up VPN if needed, mount the share, and start the worker container
sudo scripts/local_worker.sh serve

# Request graceful shutdown, stop the compose stack, and unmount the share
sudo WORKER_ID=tiago-laptop scripts/local_worker.sh stop
```

Tune the behaviour by exporting variables (e.g. `NFS_SERVER`, `MOUNT_POINT`,
`WORKER_ID`, `WORKER_IMAGE`, `OPEVA_SERVER`) before running the script, or create
an untracked `.local-worker.env` in this repo:

```bash
WORKER_ID=tiago-laptop
OPEVA_SERVER=http://193.136.62.78:8011
NFS_SERVER=193.136.62.78
NFS_EXPORT=/opt/opeva_shared_data
MOUNT_POINT=/mnt/opeva_shared
VPN_CONNECTION=deinet
VPN_REQUIRED=1
VPN_WATCHDOG=1
VPN_TARGET=193.136.62.78
WORKER_AGENT_IMAGE=job_worker_agent:local
WORKER_JOB_IMAGE=calof/opeva_simulator:latest
WORKER_ENABLE_GPU=true
WORKER_REQUIRE_GPU=true
SHUTDOWN_TIMEOUT=900
PULL_BEFORE_START=0
```

Then the daily command is:

```bash
sudo scripts/local_worker.sh serve
```

`serve` runs `nmcli connection up "$VPN_CONNECTION"` if the VPN is not already
active, then mounts NFS and starts a lightweight VPN/NFS watchdog. The watchdog
only attempts recovery checks; it does not stop jobs or force-unmount the share.
For this to work without the graphical session, the VPN credentials must be
usable by NetworkManager as a system/headless connection.

The compose definition lives in `docker-compose.local.yml`. The worker should
point to the orchestrator using the address that is accessible from the laptop
(typically the server's public/VPN-routed IP, e.g. `http://193.136.62.78:8011`).
The worker uses the shared NFS mount for configs, logs, progress and results.
The local helper enables `WORKER_REMAP_DATA_VOLUME=true`, which remaps the
orchestrator-provided `/data` bind to the laptop's local `OPEVA_SHARED_DIR`, so
the laptop mount does not need to use the same absolute path as the server.

If you prefer a one-liner without `export`, prefix the command:

```bash
sudo WORKER_ID=tiago-laptop OPEVA_SERVER=http://193.136.62.78:8011 WORKER_ENABLE_GPU=true WORKER_REQUIRE_GPU=true SHUTDOWN_TIMEOUT=900 scripts/local_worker.sh serve
```

While the worker runs you can:

- Inspect active job containers: `sudo docker ps --filter name=job_tiago-laptop`.
- Watch job logs: `sudo tail -f /mnt/opeva_shared/jobs/<job_id>/logs/<job_id>.log`.
- Check the worker state/mount: `sudo WORKER_ID=tiago-laptop scripts/local_worker.sh status`.
- Watch worker logs: `sudo WORKER_ID=tiago-laptop scripts/local_worker.sh logs`.
- Bring up the VPN only: `sudo scripts/local_worker.sh vpn`.
- Mount/unmount only: `sudo scripts/local_worker.sh mount` / `sudo scripts/local_worker.sh umount`.
- pull latest image `docker pull calof/job_worker_agent:latest`

Need to abort immediately? `sudo WORKER_ID=tiago-laptop scripts/local_worker.sh stop --force`
removes the worker and any job containers without waiting for the current job to finish,
and posts a `failed` status with `error="force-stop"` for each running job.
The normal `stop` command is controlled: it sends `SIGUSR1`, the worker stops
accepting new jobs, finishes any current job, reports the final status, exits,
and only then the wrapper brings the compose stack down and unmounts the share.
The wrapper disables Docker's restart policy before the signal so a clean worker
exit is not restarted by `restart: unless-stopped`.
If `SHUTDOWN_TIMEOUT` expires, the wrapper leaves the worker running instead of
forcing it; re-run `stop`, increase `SHUTDOWN_TIMEOUT`, or use `stop --force`.
The worker always passes the job id as `--job_id <value>` to match the simulator
entrypoint. GPU requests are controlled explicitly via `WORKER_ENABLE_GPU=true`
(recommended only on GPU-capable hosts). By default, if enabled, the worker falls
back to CPU when Docker cannot satisfy GPU allocation. Set
`WORKER_REQUIRE_GPU=true` on a GPU-serving laptop to fail fast instead.

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
  -e OPEVA_SERVER=http://job_orchestrator_agent:8011 \
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
| `OPEVA_SERVER` | Job Orchestrator base URL (default `http://localhost:8011`). |
| `WORKER_ID` | Worker identifier; defaults to container hostname. |
| `WORKER_EXECUTOR` | `docker` (default), `deucalion` or `union`. |
| `WORKER_VERSION` | Optional override for the version reported to the orchestrator; otherwise the installed package version is used. |
| `OPEVA_SHARED_DIR` | Local path to the mounted NFS share. |
| `WORKER_ENABLE_GPU` | Requests GPU access for Docker jobs. |
| `WORKER_REQUIRE_GPU` | Fails jobs if Docker cannot satisfy the GPU request instead of falling back to CPU. |
| `WORKER_REMAP_DATA_VOLUME` | Remaps orchestrator-provided `/data` volume binds to local `OPEVA_SHARED_DIR` (default `false`; local helper sets `true`). |
| `WORKER_DOCKER_PRUNE_OLD_JOB_IMAGES` | When true, removes unused old tags from the same job image repository before pulling the next job image; useful for small disks. |
| `POLL_INTERVAL` | Seconds between queue polls when idle. |
| `WORKER_HEARTBEAT_INTERVAL` | Heartbeat interval in seconds. |
| `STATUS_POLL_INTERVAL` | How often to check job status while running (seconds). |
| `LOG_LEVEL` | Python logging level (`INFO`, `DEBUG`, …). |
| `WORKER_EXIT_AFTER_JOB` | Set to `1`/`true` to stop polling after the next job finishes. |

Cadence alignment with the orchestrator:
- Typical setup: `WORKER_HEARTBEAT_INTERVAL=30` with orchestrator `HOST_HEARTBEAT_TTL=60`.
- More responsive host status in UI: `WORKER_HEARTBEAT_INTERVAL=15` and orchestrator `HOST_HEARTBEAT_TTL=45`.

Runtime version reporting:
- Every heartbeat includes `info.worker_version`.
- Every `POST /api/agent/job-status` includes `worker_version`.
- The orchestrator exposes the latest value in `/hosts` under `hosts.<worker_id>.info.worker_version`, plus `last_status_*` fields for the last status publication.

Deucalion-only variables:

| Variable | Description |
|----------|-------------|
| `DEUCALION_SSH_HOST` / `DEUCALION_SSH_USER` | SSH endpoint for Deucalion login node. |
| `DEUCALION_SSH_PORT` | SSH port (default `22`). |
| `DEUCALION_SSH_KEY_PATH` | Path inside container to private key (recommended mount RO). |
| `DEUCALION_SSH_KNOWN_HOSTS` | Path inside container to known_hosts file (recommended mount RO). |
| `DEUCALION_REMOTE_ROOT` | Remote root directory (default `/projects/F202508843CPCAA0/tiagocalof`). |
| `DEUCALION_SIF_REPOSITORY` | OCI repository containing pre-built SIF artifacts (default `calof/opeva_simulator_sif`). |
| `DEUCALION_SIF_REGISTRY` | OCI registry host for SIF artifacts (default `docker.io`). |
| `DEUCALION_SIF_REMOTE_CACHE_DIR` | Remote versioned cache directory for downloaded `.sif` files (default `<remote_root>/images/cache`). |
| `DEUCALION_SIF_LOCAL_CACHE_DIR` | Local worker cache directory for pulled artifacts (default `/tmp/opeva_sif_artifacts`). |
| `DEUCALION_SIF_PULL_TIMEOUT_SECONDS` | Timeout for each ORAS pull attempt (default `1800`). |
| `DEUCALION_SIF_PULL_RETRIES` | Retry count for transient artifact pull failures (default `3`). |
| `DEUCALION_SIF_PULL_RETRY_BACKOFF` | Base backoff seconds for SIF pull retries (default `5`). |
| `DEUCALION_SIF_LOCK_WAIT_TIMEOUT_SECONDS` | Max wait for remote per-tag lock acquisition (default `900`). |
| `DEUCALION_SIF_LOCK_POLL_INTERVAL` | Poll interval while waiting for remote lock (default `2`). |
| `DEUCALION_SIF_LOCK_STALE_SECONDS` | Age after which stale lock directories are cleaned up (default `1800`). |
| `DEUCALION_SIF_PATH` | Optional legacy override path; normally the worker computes a versioned cache path from image tag. |
| `DEUCALION_SIF_COMMAND_MODE` | Singularity mode (`run` or `exec`). Default: `run`. |
| `DEUCALION_CONTAINER_WORKDIR` | Working directory inside container for Singularity actions (default `/app`). |
| `DEUCALION_DATASET_COPY_RETRIES` | Retries for dataset SCP sync (default `3`). |
| `DEUCALION_DATASET_COPY_RETRY_BACKOFF` | Base backoff seconds for dataset copy retries (default `2.0`). |
| `DEUCALION_DATASET_COPY_TIMEOUT_SECONDS` | Timeout for directory dataset copy (default `1800`). |
| `DEUCALION_POLL_INTERVAL` | Slurm state poll interval in seconds (default `10`). |
| `DEUCALION_SYNC_INTERVAL` | Remote log sync interval in seconds (default `15`). |
| `DEUCALION_UNREACHABLE_GRACE_SECONDS` | Grace window before failing unreachable jobs (default `900`). |
| `DEUCALION_UNKNOWN_STATE_TIMEOUT_SECONDS` | Max seconds to tolerate continuous `UNKNOWN` state before failing (`300`). |
| `DEUCALION_SLURM_ACCOUNT_CPU/GPU` | Default Slurm accounts (`f202508843cpcaa0x` / `f202508843cpcaa0g`). |
| `DEUCALION_SLURM_PARTITION_CPU/GPU` | Default partitions (`normal-x86` / `normal-a100-80`). |
| `DEUCALION_SLURM_TIME` | Default time limit, e.g. `04:00:00`. |
| `DEUCALION_SLURM_CPUS_PER_TASK` | Default CPU cores per task. |
| `DEUCALION_SLURM_MEM_GB` | Default memory in GB. |
| `DEUCALION_SLURM_GPUS` | Default GPU count (`0` means CPU job). |

Per-job overrides in YAML (`execution.deucalion`) support:
- `command_mode: run|exec` (default `run`)
- `datasets: [datasets/...,...]` (relative to shared root; copied to
  `<remote_root>/datasets/...` only when missing)
- existing keys: `account`, `partition`, `time`, `cpus_per_task`, `mem_gb`,
  `gpus`, `modules`, `sif_path`, `required_paths`

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
`calof/job_worker_agent:<sha>` (and `:latest` on `main`, plus `:vX.Y.Z` for release tags). Configure the secrets
`DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` in GitHub to enable the push step.
