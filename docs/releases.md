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
