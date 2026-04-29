from __future__ import annotations

import requests

from alpaca_lab.config import LabSettings
from alpaca_lab.notifications.ntfy import NtfyNotifier


def test_ntfy_notifier_is_disabled_without_topic() -> None:
    notifier = NtfyNotifier(LabSettings())

    assert notifier.enabled is False


def test_ntfy_notifier_posts_plain_text_to_topic(monkeypatch) -> None:
    captured = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

    class _Session:
        def post(self, url, data, headers, timeout):
            captured["url"] = url
            captured["data"] = data
            captured["headers"] = headers
            captured["timeout"] = timeout
            return _Response()

    settings = LabSettings.model_validate(
        {
            "ntfy_topic": "codexalpaca/private-room",
            "ntfy_access_token": "test-token",
        }
    )
    notifier = NtfyNotifier(settings, session=_Session())

    assert notifier.send("**Morning Check**\nTrader is live.") is True
    assert captured["url"] == "https://ntfy.sh/codexalpaca/private-room"
    assert captured["data"] == b"**Morning Check**\nTrader is live."
    assert captured["headers"]["Content-Type"] == "text/plain; charset=utf-8"
    assert captured["headers"]["Title"] == "Morning Check"
    assert captured["headers"]["Authorization"] == "Bearer test-token"
    assert captured["timeout"] == settings.request_timeout_seconds


def test_ntfy_notifier_returns_false_after_request_failure() -> None:
    class _Session:
        def post(self, *_args, **_kwargs):
            raise requests.RequestException("publish failed")

    settings = LabSettings.model_validate({"ntfy_topic": "codexalpaca/fail"})
    notifier = NtfyNotifier(settings, session=_Session())

    assert notifier.send("hello") is False
