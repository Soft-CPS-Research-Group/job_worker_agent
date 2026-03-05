from __future__ import annotations

import shlex
from dataclasses import dataclass

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
    sacct_out = ssh.run(
        f"sacct -X -j {shlex.quote(slurm_job_id)} --format=State,ExitCode --parsable2 -n",
        timeout=30,
        check=False,
    )
    for line in sacct_out.splitlines():
        raw = line.strip()
        if not raw:
            continue
        parts = raw.split("|")
        state = _normalize_state(parts[0]) if parts else ""
        if not state:
            continue
        exit_code = None
        if len(parts) > 1:
            # format usually "0:0"
            code_text = parts[1].strip().split(":")[0]
            try:
                exit_code = int(code_text)
            except ValueError:
                exit_code = None
        return SlurmState(state=state, exit_code=exit_code)

    return SlurmState(state="UNKNOWN", exit_code=None)
