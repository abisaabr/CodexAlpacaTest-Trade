from __future__ import annotations

import ssl

import requests

from alpaca_lab.config import LabSettings
from alpaca_lab.notifications.discord import DiscordWebhookNotifier, _TlsHttpAdapter


def test_discord_notifier_uses_tls_hardened_https_adapter() -> None:
    notifier = DiscordWebhookNotifier(LabSettings())

    adapter = notifier.session.get_adapter("https://discord.com/api/webhooks/test")

    assert isinstance(adapter, _TlsHttpAdapter)
    assert adapter._ssl_context.minimum_version == ssl.TLSVersion.TLSv1_2


def test_discord_notifier_uses_powershell_fallback_after_request_failure(monkeypatch) -> None:
    class FailingSession:
        def post(self, *_args, **_kwargs):
            raise requests.RequestException("handshake failure")

    notifier = DiscordWebhookNotifier(
        LabSettings.model_validate({"discord_webhook_url": "https://discord.com/api/webhooks/test"}),
        session=FailingSession(),
    )
    called = {}

    def fake_fallback(webhook_url: str, content: str) -> None:
        called["webhook_url"] = webhook_url
        called["content"] = content
        return None

    monkeypatch.setattr(notifier, "_send_via_powershell", fake_fallback)

    assert notifier.send("hello from fallback") is True
    assert called == {
        "webhook_url": "https://discord.com/api/webhooks/test",
        "content": "hello from fallback",
    }
