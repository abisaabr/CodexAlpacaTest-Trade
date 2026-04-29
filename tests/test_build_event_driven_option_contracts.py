from __future__ import annotations

import pandas as pd
import pytest

from scripts.build_event_driven_option_contracts import select_event_driven_contracts


def test_event_driven_contract_selection_uses_strategy_entry_reference() -> None:
    contracts = pd.DataFrame(
        {
            "symbol": ["GLD260424P00095000", "GLD260424P00100000", "GLD260424C00095000"],
            "underlying_symbol": ["GLD", "GLD", "GLD"],
            "expiration_date": ["2026-04-24", "2026-04-24", "2026-04-24"],
            "option_type": ["put", "put", "call"],
            "strike_price": [95.0, 100.0, 95.0],
        }
    )
    trades = pd.DataFrame(
        {
            "entry_time": ["2026-04-21T14:00:00Z"],
            "entry_price": [95.2],
        }
    )

    selected = select_event_driven_contracts(
        contracts=contracts,
        trades=trades,
        symbol="GLD",
        option_type="put",
        candidate_variant_id="candidate-1",
        min_dte=1,
        max_dte=10,
        strike_steps=0,
    )

    assert selected["symbol"].tolist() == ["GLD260424P00095000"]
    row = selected.iloc[0]
    assert row["reference_timestamp"] == pd.Timestamp("2026-04-21T14:00:00Z")
    assert row["reference_price"] == 95.2
    assert row["atm_strike"] == 95.0
    assert row["relative_strike_step"] == 0
    assert row["dte"] == 3
    assert row["inventory_status"] is pd.NA
    assert "reference=strategy_entry_time" in row["selection_reason"]
    assert "candidate_variant_id=candidate-1" in row["selection_reason"]


def test_event_driven_contract_selection_deduplicates_contracts_by_trade_date() -> None:
    contracts = pd.DataFrame(
        {
            "symbol": ["GLD260424P00095000"],
            "underlying_symbol": ["GLD"],
            "expiration_date": ["2026-04-24"],
            "option_type": ["put"],
            "strike_price": [95.0],
            "inventory_status": ["inactive"],
        }
    )
    trades = pd.DataFrame(
        {
            "entry_time": ["2026-04-21T14:00:00Z", "2026-04-21T15:00:00Z"],
            "entry_price": [95.2, 95.1],
        }
    )

    selected = select_event_driven_contracts(
        contracts=contracts,
        trades=trades,
        symbol="gld",
        option_type="PUT",
        candidate_variant_id="candidate-1",
        min_dte=1,
        max_dte=10,
        strike_steps=0,
    )

    assert len(selected) == 1
    assert selected.iloc[0]["inventory_status"] == "inactive"


def test_event_driven_contract_selection_rejects_negative_strike_steps() -> None:
    with pytest.raises(ValueError, match="strike_steps"):
        select_event_driven_contracts(
            contracts=pd.DataFrame({"symbol": ["X"]}),
            trades=pd.DataFrame({"entry_time": ["2026-04-21T14:00:00Z"], "entry_price": [1.0]}),
            symbol="GLD",
            option_type="put",
            candidate_variant_id="candidate-1",
            min_dte=1,
            max_dte=10,
            strike_steps=-1,
        )
