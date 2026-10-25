# 0.7.0 — one worker control API

Status polling always uses `/api/agent/status/{job_id}`. The temporary `WORKER_EXECUTION_API_V1` switch is removed. Requires Orchestrator 1.4.0 or newer. CPU, Union and local Docker workers use the same protocol; job execution, attempt isolation and result recovery are unchanged.

Local workers must use this release, mount the shared filesystem and reach the Orchestrator. The laptop helper's existing start/stop commands remain supported. VPN addresses must match the server's NFS export access list; never open the export to all clients to fix a refused mount.
