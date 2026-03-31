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
    partition: str | None = None
    reason: str | None = None
    submit_time: str | None = None
    start_time: str | None = None
    elapsed: str | None = None
    time_left: str | None = None
    priority: int | None = None
    user: str | None = None
    nodes: int | None = None
    cpus: int | None = None
    queue_position: int | None = None
    jobs_ahead: int | None = None
    pending_jobs_in_partition: int | None = None


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


def _clean_field(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    if text.upper() in {"N/A", "UNKNOWN", "NONE"}:
        return None
    return text


def _parse_int(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
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
    squeue_out = ssh.run(
        f"squeue -h -j {shlex.quote(slurm_job_id)} -o '%T|%P|%R|%V|%S|%M|%L|%Q|%u|%D|%C'",
        timeout=30,
        check=False,
    )
    for line in squeue_out.splitlines():
        raw = line.strip()
        if not raw:
            continue
        parts = raw.split("|", 10)
        if len(parts) < 11:
            parts.extend([""] * (11 - len(parts)))
        state = _normalize_state(parts[0])
        if state:
            partition = _clean_field(parts[1])
            pending_jobs: list[str] = []
            if state == "PENDING" and partition:
                pending_raw = ssh.run(
                    f"squeue -h -p {shlex.quote(partition)} -t PD -o %i",
                    timeout=30,
                    check=False,
                )
                pending_jobs = [item.strip() for item in pending_raw.splitlines() if item.strip()]
            queue_position = None
            jobs_ahead = None
            pending_jobs_in_partition = len(pending_jobs) if pending_jobs else None
            for idx, queued_job_id in enumerate(pending_jobs, start=1):
                if queued_job_id == slurm_job_id:
                    queue_position = idx
                    jobs_ahead = idx - 1
                    break

            return SlurmState(
                state=state,
                partition=partition,
                reason=_clean_field(parts[2]),
                submit_time=_clean_field(parts[3]),
                start_time=_clean_field(parts[4]),
                elapsed=_clean_field(parts[5]),
                time_left=_clean_field(parts[6]),
                priority=_parse_int(parts[7]),
                user=_clean_field(parts[8]),
                nodes=_parse_int(parts[9]),
                cpus=_parse_int(parts[10]),
                queue_position=queue_position,
                jobs_ahead=jobs_ahead,
                pending_jobs_in_partition=pending_jobs_in_partition,
            )

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
