# 0.6.1 — execution API compatibility

Set `WORKER_EXECUTION_API_V1=1` with Orchestrator 1.4.0 or newer. The worker polls
`/api/agent/status/{job_id}` for cancellation and recovery, keeping client-owned
jobs on the trusted worker control plane. Dispatch, attempt tokens, shared files,
Union transfers and result publication are unchanged. Default polling remains
`/status/{job_id}` for installations upgrading the server first.

This worker never accesses AI Training or its database. Training is an execution
client. CPU and Union use the same code release; Deucalion/Jetson are not part of
this deployment. No automatic data retention is introduced.
