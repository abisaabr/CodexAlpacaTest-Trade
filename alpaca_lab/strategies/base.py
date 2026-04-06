from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

REQUIRED_SIGNAL_COLUMNS = ("signal", "stop_pct", "target_pct", "timeout_bars", "size_fraction")


class StrategyValidationError(ValueError):
    """Raised when the strategy receives data that does not fit its contract."""


@dataclass(slots=True)
class BaseStrategy(ABC):
    name: str
    instrument_type: str = "stock"
    contract_multiplier: float = 1.0

    @abstractmethod
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        """Return a bar-aligned frame with at least the required signal columns."""

    def validate_bars(self, bars: pd.DataFrame, required_columns: tuple[str, ...]) -> None:
        missing = [column for column in required_columns if column not in bars.columns]
        if missing:
            raise StrategyValidationError(
                f"{self.name} requires columns {missing}, "
                "but they were not present in the input frame."
            )

    def finalize_signal_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        for column in REQUIRED_SIGNAL_COLUMNS:
            if column not in frame.columns:
                raise StrategyValidationError(
                    f"{self.name} did not provide the required signal column {column}."
                )
        if "contract_multiplier" not in frame.columns:
            frame["contract_multiplier"] = self.contract_multiplier
        return frame
