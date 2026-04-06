from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from itertools import islice
from typing import Iterable, Iterator, Sequence, TypeVar
from zoneinfo import ZoneInfo


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class DateChunk:
    start_date: date
    end_date: date
    index: int

    @property
    def label(self) -> str:
        return f"{self.start_date.isoformat()}__{self.end_date.isoformat()}"


def iter_date_chunks(start_date: date, end_date: date, *, chunk_days: int) -> Iterator[DateChunk]:
    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive.")
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date.")

    cursor = start_date
    index = 0
    while cursor <= end_date:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end_date)
        yield DateChunk(start_date=cursor, end_date=chunk_end, index=index)
        cursor = chunk_end + timedelta(days=1)
        index += 1


def iter_dates(start_date: date, end_date: date) -> Iterator[date]:
    for chunk in iter_date_chunks(start_date, end_date, chunk_days=1):
        yield chunk.start_date


def market_session_bounds(
    trade_date: date,
    *,
    timezone_name: str = "America/New_York",
) -> tuple[datetime, datetime]:
    market_zone = ZoneInfo(timezone_name)
    session_start = datetime.combine(trade_date, time(hour=9, minute=30), tzinfo=market_zone)
    session_end = datetime.combine(trade_date, time(hour=16, minute=0), tzinfo=market_zone)
    return session_start.astimezone(timezone.utc), session_end.astimezone(timezone.utc)


def batched(items: Sequence[T] | Iterable[T], batch_size: int) -> Iterator[list[T]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive.")
    iterator = iter(items)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch
