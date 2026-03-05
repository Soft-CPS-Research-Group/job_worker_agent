from worker_agent.deucalion.config import resolve_deucalion_job_config


def test_resolve_config_cpu_defaults():
    cfg = resolve_deucalion_job_config(
        config={"execution": {"deucalion": {"sif_path": "/remote/sim.sif"}}},
        env={},
    )
    assert cfg.profile.gpus == 0
    assert cfg.profile.account == "f202508843cpcaa0x"
    assert cfg.profile.partition == "normal-x86"


def test_resolve_config_gpu_and_yaml_override():
    cfg = resolve_deucalion_job_config(
        config={
            "execution": {
                "deucalion": {
                    "sif_path": "/remote/sim.sif",
                    "gpus": 1,
                    "partition": "dev-a100-80",
                    "account": "custom-gpu-account",
                    "time": "01:00:00",
                }
            }
        },
        env={
            "DEUCALION_SLURM_ACCOUNT_GPU": "f202508843cpcaa0g",
            "DEUCALION_SLURM_PARTITION_GPU": "normal-a100-80",
        },
    )
    assert cfg.profile.gpus == 1
    assert cfg.profile.partition == "dev-a100-80"
    assert cfg.profile.account == "custom-gpu-account"
    assert cfg.profile.time_limit == "01:00:00"
