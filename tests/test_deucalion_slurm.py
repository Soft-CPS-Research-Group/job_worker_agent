from worker_agent.deucalion.slurm import query_state


class FakeSSH:
    def __init__(self, squeue_out: str, sacct_out: str):
        self.squeue_out = squeue_out
        self.sacct_out = sacct_out

    def run(self, command, timeout=30, check=False):  # noqa: ARG002
        if command.startswith("squeue "):
            return self.squeue_out
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
