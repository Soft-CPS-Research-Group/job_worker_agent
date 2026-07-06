from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest

from worker_agent.deucalion.ssh_client import SSHClient, SSHCommandError, SSHSettings


def _settings() -> SSHSettings:
    return SSHSettings(
        host="example.org",
        user="alice",
        port=22,
        key_path="/tmp/key",
        known_hosts_path="/tmp/known_hosts",
    )


def test_ssh_uses_lowercase_p_for_port(monkeypatch):
    captured = {}

    def _fake_run(cmd, text, capture_output, timeout):
        captured["cmd"] = cmd
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("worker_agent.deucalion.ssh_client.subprocess.run", _fake_run)
    client = SSHClient(_settings())
    client.run("echo hi")

    assert captured["cmd"][0] == "ssh"
    assert "-p" in captured["cmd"]
    assert "22" in captured["cmd"]


def test_scp_uses_uppercase_p_for_port(monkeypatch):
    captured = {}

    def _fake_run(cmd, text, capture_output, timeout):
        captured["cmd"] = cmd
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("worker_agent.deucalion.ssh_client.subprocess.run", _fake_run)
    client = SSHClient(_settings())
    client.copy_to("/tmp/local", "/remote/path", recursive=True)

    assert captured["cmd"][0] == "scp"
    assert "-P" in captured["cmd"]
    assert "-p" not in captured["cmd"]
    assert "22" in captured["cmd"]


def test_ssh_run_timeout_raises_ssh_command_error(monkeypatch):
    def _fake_run(cmd, text, capture_output, timeout):  # noqa: ARG001
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)

    monkeypatch.setattr("worker_agent.deucalion.ssh_client.subprocess.run", _fake_run)
    client = SSHClient(_settings())

    with pytest.raises(SSHCommandError, match="timed out after 30s"):
        client.run("squeue -h -p normal-a100-80 -t PD -o %i", timeout=30, check=False)
