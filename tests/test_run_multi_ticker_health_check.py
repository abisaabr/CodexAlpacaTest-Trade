from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from alpaca_lab.execution.ownership import FileOwnershipLease, GenerationMatchOwnershipLease
from alpaca_lab.multi_ticker_portfolio import default_portfolio_config

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "run_multi_ticker_health_check.py"
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("run_multi_ticker_health_check", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_build_health_check_ownership_lease_defaults_to_file_backend() -> None:
    lease = MODULE.build_health_check_ownership_lease(default_portfolio_config())

    assert isinstance(lease, FileOwnershipLease)


def test_build_health_check_ownership_lease_supports_gcs_backend(monkeypatch) -> None:
    class _StoreStub:
        def read(self):  # pragma: no cover - interface placeholder
            return None

        def create_if_absent(self, payload):
            return "1"

        def replace_if_generation(self, *, generation, payload):
            return "2"

        def delete_if_generation(self, *, generation):
            return None

    config = default_portfolio_config().model_copy(
        update={
            "ownership": default_portfolio_config().ownership.model_copy(
                update={
                    "lease_backend": "gcs_generation_match",
                    "gcs_lease_uri": "gs://codexalpaca-control-us/leases/paper-execution/lease.json",
                    "machine_label": "vm-execution-paper-01",
                }
            )
        }
    )
    monkeypatch.setattr(
        MODULE.GCSGenerationMatchLeaseStore,
        "from_gcs_uri",
        lambda gcs_uri: _StoreStub(),
    )

    lease = MODULE.build_health_check_ownership_lease(config)

    assert isinstance(lease, GenerationMatchOwnershipLease)
    assert lease.lease_path == "gs://codexalpaca-control-us/leases/paper-execution/lease.json"
