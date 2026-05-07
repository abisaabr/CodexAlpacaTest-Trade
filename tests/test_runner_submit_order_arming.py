from __future__ import annotations

from argparse import Namespace
from types import SimpleNamespace

from scripts.run_multi_ticker_eod_close_guard import (
    resolve_submit_paper_orders as resolve_eod_close_guard_submit,
)
from scripts.run_multi_ticker_portable_daemon import (
    resolve_submit_paper_orders as resolve_portable_daemon_submit,
)
from scripts.run_multi_ticker_portfolio_paper_trader import (
    resolve_submit_paper_orders as resolve_multi_ticker_submit,
)
from scripts.run_qqq_portfolio_paper_trader import (
    resolve_submit_paper_orders as resolve_qqq_submit,
)


def _config(
    *,
    submit_paper_orders: bool,
    paper_order_arming_mode: str = "cli_flag_only",
) -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(
            submit_paper_orders=submit_paper_orders,
            paper_order_arming_mode=paper_order_arming_mode,
        )
    )


def test_multi_ticker_entrypoint_requires_explicit_submit_flag() -> None:
    args = Namespace(
        submit_paper_orders=False,
        no_submit_paper_orders=False,
        startup_preflight=False,
    )

    assert resolve_multi_ticker_submit(args, _config(submit_paper_orders=True)) is False


def test_multi_ticker_entrypoint_allows_explicit_config_arming_mode() -> None:
    config = _config(submit_paper_orders=True, paper_order_arming_mode="config_explicit")

    assert (
        resolve_multi_ticker_submit(
            Namespace(
                submit_paper_orders=False,
                no_submit_paper_orders=False,
                startup_preflight=False,
            ),
            config,
        )
        is True
    )
    assert (
        resolve_multi_ticker_submit(
            Namespace(
                submit_paper_orders=False,
                no_submit_paper_orders=True,
                startup_preflight=False,
            ),
            config,
        )
        is False
    )
    assert (
        resolve_multi_ticker_submit(
            Namespace(
                submit_paper_orders=False,
                no_submit_paper_orders=False,
                startup_preflight=True,
            ),
            config,
        )
        is False
    )


def test_multi_ticker_entrypoint_honors_submit_flag_unless_suppressed() -> None:
    config = _config(submit_paper_orders=False)

    assert (
        resolve_multi_ticker_submit(
            Namespace(
                submit_paper_orders=True,
                no_submit_paper_orders=False,
                startup_preflight=False,
            ),
            config,
        )
        is True
    )
    assert (
        resolve_multi_ticker_submit(
            Namespace(
                submit_paper_orders=True,
                no_submit_paper_orders=True,
                startup_preflight=False,
            ),
            config,
        )
        is False
    )
    assert (
        resolve_multi_ticker_submit(
            Namespace(
                submit_paper_orders=True,
                no_submit_paper_orders=False,
                startup_preflight=True,
            ),
            config,
        )
        is False
    )


def test_daemon_and_close_guard_require_explicit_submit_flag() -> None:
    config = _config(submit_paper_orders=True)
    args = Namespace(submit_paper_orders=False)

    assert resolve_portable_daemon_submit(args, config) is False
    assert resolve_eod_close_guard_submit(args, config) is False
    assert resolve_qqq_submit(args, config) is False


def test_daemon_and_close_guard_allow_explicit_submit_flag() -> None:
    config = _config(submit_paper_orders=False)
    args = Namespace(submit_paper_orders=True)

    assert resolve_portable_daemon_submit(args, config) is True
    assert resolve_eod_close_guard_submit(args, config) is True
    assert resolve_qqq_submit(args, config) is True
