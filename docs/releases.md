# Releases

This is the operational release log for `job_worker_agent`. Every version bump should record runtime-contract changes, validation and deployment impact.

Default release owner: [@calofonseca](https://github.com/calofonseca).

## Version Policy

| Type | Use when | Example |
| --- | --- | --- |
| Patch | Compatible fixes and telemetry/reporting improvements. | `0.4.1 -> 0.4.2` |
| Minor | New compatible executor capability or worker contract. | `0.4.x -> 0.5.0` |
| Major | Breaking CLI, API or executor contract change. | `0.x -> 1.0.0` |

## Release Checklist

1. Update `pyproject.toml`.
2. Update `worker_agent/version.py`.
3. Update this file with release notes.
4. Update README/runtime docs when public contracts change.
5. Run validation:
   - `.venv/bin/pytest`
   - `python3 -m compileall worker_agent`
   - `docker build -t job_worker_agent:test .`
6. Commit and tag.
7. Push `main` and tag `vX.Y.Z`.
8. Verify GitHub Actions pushed Docker tags:
   - `calof/job_worker_agent:<commit-sha>`
   - `calof/job_worker_agent:latest` from `main`
   - `calof/job_worker_agent:vX.Y.Z` from release tags

## v0.5.1 - 2026-07-16

Release owner: [@calofonseca](https://github.com/calofonseca).

### Summary

Hardens long-running Union jobs against interrupted live logs, temporary Union
control-plane/object-store failures, bridge restarts and delayed result
metadata.

### Changed

- Reconnects interrupted Union log streams with bounded backoff and resumes
  from the persisted line/event cursor, keeping UI logs and progress current.
- Limits temporary artifact-signer Run names to Union's 30-character maximum.
- Drains terminal events before closing the log stream for short Runs.
- Adopts the deterministic existing Run when submission succeeded remotely but
  the client lost the response, preventing duplicate Algorithms executions.
- Retries temporary input/result object-store operations and treats cancel
  marker checks and remote cleanup as best effort.
- Retries delayed result metadata before failing a completed Run and streams
  large final logs without loading the entire file into bridge memory.
- Re-authenticates Device Flow sessions without discarding active remote Runs,
  and tolerates eventual consistency when a newly submitted Run is queried.
- Aborts remote work when the orchestrator already reports the job as stopped.
- Extends the default Algorithms timeout from 7 to 30 days.
- Reports the GPU model assigned by Union once per running job so host details
  can distinguish resources such as H200 and RTX PRO 6000 without polling.
- Worker version is now reported as `0.5.1`.

### Validation

- `.venv/bin/pytest`: pass (`134 passed`).
- Dedicated Union image build and import smoke: pass.

## v0.5.0 - 2026-07-14

Release owner: [@calofonseca](https://github.com/calofonseca).

### Summary

Adds the Union INESC TEC GPU executor while preserving the existing Docker,
Deucalion and Jetson execution paths.

### Changed

- Added a recoverable `union` executor with deterministic run IDs, live logs,
  progress events, controlled cancellation and validated artifact installation.
- Added the dedicated amd64 `Dockerfile.union` image and `union-*` CI tags.
- Added input packaging for the resolved config and referenced datasets only.
- Added durable Union run state and restart recovery across setup and execution.
- Added capability-negotiated attempt fencing so stale workers cannot publish
  status into a requeued execution. Superseded Docker, Slurm and Union
  executions are actively terminated; legacy orchestrators and workers remain
  usable during rolling upgrades.
- Worker version is now reported as `0.5.0`.

### Compatibility

- Existing worker images still use the original `Dockerfile` and dependencies.
- The Union dependencies are isolated in the optional `union` extra and the
  dedicated image.
- The orchestrator must include `union-inesctec` in `AVAILABLE_HOSTS`.

### Validation

- `.venv/bin/pytest`: pass (`105 passed`).
- Real Union GPU smoke with `calof/opeva_simulator:sha-91811d4`: pass.
- Two generic Union GPU tasks: confirmed parallel start on separate GPUs.

## v0.4.1 - 2026-05-16

Release owner: [@calofonseca](https://github.com/calofonseca).

### Summary

Patch release that makes the worker runtime version visible to the Job Orchestrator on both heartbeat and job-status publications.

### Changed

- `job-status` payloads now include `worker_version`.
- Default local worker server URL now points to the Job Orchestrator port `8011`.
- CI now publishes Docker tag images for `v*` release tags.

### Compatibility

- Backward compatible with existing orchestrator versions; unknown extra fields are ignored by older servers.
- New orchestrator versions expose the latest worker version under `/hosts`.

### Validation

- `.venv/bin/pytest`: pass (`67 passed`)
- `python3 -m compileall worker_agent`: pass
- `docker build -t job_worker_agent:test .`: pass
- `docker run --rm --entrypoint python job_worker_agent:test -c "from worker_agent import __version__; print(__version__)"`: pass (`0.4.1`)
