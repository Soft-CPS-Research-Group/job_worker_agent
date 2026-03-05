from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import Optional

from .ssh_client import SSHClient


ACTIVE_STATES = {
    "PENDING",
    "CONFIGURING",
    "COMPLETING",
    "RUNNING",
    "STAGE_OUT",
    "RESIZING",
    "SUSPENDED",
}

TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
}


def _normalize_state(value: str) -> str:
    raw = value.strip().upper()
    if not raw:
        return raw
    raw = raw.split()[0]
    raw = raw.split("+")[0]
    return raw


@dataclass
class SlurmState:
    state: str
    exit_code: int | None = None


def _parse_exit_code(value: str) -> Optional[int]:
    text = value.strip()
    if not text:
        return None
    code_text = text.split(":", 1)[0].strip()
    if not code_text:
        return None
    try:
        return int(code_text)
    except ValueError:
        return None


def sbatch_submit(ssh: SSHClient, remote_script_path: str, remote_workdir: str) -> str:
    cmd = f"cd {shlex.quote(remote_workdir)} && sbatch --parsable {shlex.quote(remote_script_path)}"
    output = ssh.run(cmd, timeout=60)
    if not output:
        raise RuntimeError("Slurm submit returned empty output")
    # Slurm may return 12345 or 12345;cluster
    return output.strip().splitlines()[-1].split(";")[0].strip()


def scancel_job(ssh: SSHClient, slurm_job_id: str) -> None:
    ssh.run(f"scancel {shlex.quote(slurm_job_id)}", timeout=30, check=False)


def query_state(ssh: SSHClient, slurm_job_id: str) -> SlurmState:
    # Active/queued states from squeue
    squeue_out = ssh.run(f"squeue -h -j {shlex.quote(slurm_job_id)} -o %T", timeout=30, check=False)
    for line in squeue_out.splitlines():
        state = _normalize_state(line)
        if state:
            return SlurmState(state=state)

    # Terminal states from sacct after job leaves queue
    # Prefer the root job row (`JobIDRaw == <job_id>`) and only fallback to
    # job steps (e.g. .batch/.extern) when the root row is unavailable.
    sacct_out = ssh.run(
        f"sacct -X -j {shlex.quote(slurm_job_id)} --format=JobIDRaw,State,ExitCode --parsable2 -n",
        timeout=30,
        check=False,
    )
    fallback_state: SlurmState | None = None
    for line in sacct_out.splitlines():
        raw = line.strip()
        if not raw:
            continue
        parts = raw.split("|")
        if len(parts) < 2:
            continue
        job_id_raw = parts[0].strip()
        state = _normalize_state(parts[1])
        if not state:
            continue
        exit_code = _parse_exit_code(parts[2] if len(parts) > 2 else "")
        candidate = SlurmState(state=state, exit_code=exit_code)
        if job_id_raw == slurm_job_id:
            return candidate
        if fallback_state is None:
            fallback_state = candidate

    if fallback_state is not None:
        return fallback_state

    return SlurmState(state="UNKNOWN", exit_code=None)
