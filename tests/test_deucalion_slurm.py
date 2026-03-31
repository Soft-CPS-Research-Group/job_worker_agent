from worker_agent.deucalion.slurm import query_state


class FakeSSH:
    def __init__(self, squeue_out: str, sacct_out: str, pending_out: str = ""):
        self.squeue_out = squeue_out
        self.sacct_out = sacct_out
        self.pending_out = pending_out

    def run(self, command, timeout=30, check=False):  # noqa: ARG002
        if command.startswith("squeue ") and " -j " in command:
            return self.squeue_out
        if command.startswith("squeue ") and " -p " in command:
            return self.pending_out
        if command.startswith("sacct "):
            return self.sacct_out
        return ""


def test_query_state_prefers_root_row_over_steps():
    ssh = FakeSSH(
        squeue_out="",
        sacct_out=(
            "12345.batch|FAILED|1:0\n"
            "12345.extern|FAILED|1:0\n"
            "12345|COMPLETED|0:0\n"
        ),
    )
    state = query_state(ssh, "12345")
    assert state.state == "COMPLETED"
    assert state.exit_code == 0


def test_query_state_falls_back_to_step_when_root_missing():
    ssh = FakeSSH(
        squeue_out="",
        sacct_out=(
            "12345.batch|FAILED|2:0\n"
            "12345.extern|FAILED|2:0\n"
        ),
    )
    state = query_state(ssh, "12345")
    assert state.state == "FAILED"
    assert state.exit_code == 2


def test_query_state_pending_includes_queue_details():
    ssh = FakeSSH(
        squeue_out="PENDING|normal|Priority|2026-03-31T00:50:00|N/A|0:00|2:00:00|8800|tiago|1|16\n",
        pending_out="9999\n12345\n7777\n",
        sacct_out="",
    )
    state = query_state(ssh, "12345")
    assert state.state == "PENDING"
    assert state.partition == "normal"
    assert state.reason == "Priority"
    assert state.submit_time == "2026-03-31T00:50:00"
    assert state.start_time is None
    assert state.elapsed == "0:00"
    assert state.time_left == "2:00:00"
    assert state.priority == 8800
    assert state.user == "tiago"
    assert state.nodes == 1
    assert state.cpus == 16
    assert state.queue_position == 2
    assert state.jobs_ahead == 1
    assert state.pending_jobs_in_partition == 3
