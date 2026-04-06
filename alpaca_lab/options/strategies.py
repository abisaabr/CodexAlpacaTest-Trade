from __future__ import annotations

import pandas as pd

from alpaca_lab.options.promotion_board import PromotionCandidate


def build_long_call_candidates(
    option_frame: pd.DataFrame,
    *,
    strategy_name: str = "long_call_momentum",
    top_n: int = 5,
) -> list[PromotionCandidate]:
    if option_frame.empty:
        return []
    ranked = option_frame.copy()
    if "delta" not in ranked.columns:
        ranked["delta"] = 0.4
    if "volume" not in ranked.columns:
        ranked["volume"] = 0
    ranked["score"] = (
        ranked["delta"].fillna(0.4).astype(float)
        + ranked["volume"].fillna(0).astype(float) / 10_000.0
    )
    ranked = ranked.sort_values("score", ascending=False).head(top_n)
    candidates: list[PromotionCandidate] = []
    for row in ranked.itertuples(index=False):
        candidates.append(
            PromotionCandidate(
                symbol=row.symbol,
                asset_class="option",
                side="buy",
                strategy_name=strategy_name,
                thesis="Long premium momentum candidate selected from ranked option frame.",
                score=float(getattr(row, "score", 0.0)),
                price=float(getattr(row, "close", 0.0) or 0.0),
                limit_price=float(getattr(row, "close", 0.0) or 0.0),
                qty=1.0,
                underlying_symbol=getattr(row, "underlying_symbol", None),
                contract_multiplier=float(getattr(row, "contract_multiplier", 100.0) or 100.0),
                tags=("momentum", "option", "paper"),
            )
        )
    return candidates
