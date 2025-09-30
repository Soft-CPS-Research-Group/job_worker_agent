import sys

import pytest

from worker_agent import cli


class DummyAgent:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        DummyAgent.instances.append(self)

    def run_forever(self):
        DummyAgent.ran = True


DummyAgent.instances = []
DummyAgent.ran = False


def test_cli_uses_worker_id(monkeypatch):
    monkeypatch.setattr(cli, "WorkerAgent", DummyAgent)
    monkeypatch.setenv("WORKER_ID", "env-worker")
    monkeypatch.setenv("OPEVA_SERVER", "http://example")
    monkeypatch.setenv("OPEVA_SHARED_DIR", "/shared")
    monkeypatch.setenv("WORKER_IMAGE", "image")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("WORKER_HEARTBEAT_INTERVAL", "10")

    argv = ["cli", "--poll-interval", "1"]
    monkeypatch.setattr(sys, "argv", argv)

    cli.main()

    assert DummyAgent.ran is True
    agent_kwargs = DummyAgent.instances[0].kwargs
    assert agent_kwargs["worker_id"] == "env-worker"
    assert agent_kwargs["server_url"] == "http://example"
    assert agent_kwargs["shared_dir"] == "/shared"
    assert agent_kwargs["image"] == "image"
    assert agent_kwargs["poll_interval"] == 1.0
    assert agent_kwargs["status_poll_interval"] == 10.0
    assert agent_kwargs["heartbeat_interval"] == 10.0


def test_cli_defaults_hostname(monkeypatch):
    DummyAgent.instances.clear()
    DummyAgent.ran = False
    monkeypatch.setattr(cli, "WorkerAgent", DummyAgent)
    monkeypatch.delenv("WORKER_ID", raising=False)
    monkeypatch.setenv("OPEVA_SERVER", "http://example")
    monkeypatch.setattr(cli, "socket", type("S", (), {"gethostname": staticmethod(lambda: "host-name")})())

    argv = ["cli"]
    monkeypatch.setattr(sys, "argv", argv)

    cli.main()

    assert DummyAgent.instances[0].kwargs["worker_id"] == "host-name"
