from __future__ import annotations

import smtplib

from alpaca_lab.config import LabSettings
from alpaca_lab.notifications.email import EmailNotifier


def test_email_notifier_disabled_without_complete_settings() -> None:
    notifier = EmailNotifier(LabSettings())

    assert notifier.enabled is False


def test_email_notifier_uses_smtp_login_and_send(monkeypatch) -> None:
    settings = LabSettings.model_validate(
        {
            "email_smtp_host": "smtp.gmail.com",
            "email_smtp_port": 587,
            "email_username": "sender@example.com",
            "email_password": "app-password",
            "email_from": "sender@example.com",
            "email_to": ["dest@example.com"],
            "email_subject_prefix": "[CodexAlpaca]",
        }
    )
    notifier = EmailNotifier(settings)
    calls: dict[str, object] = {}

    class _FakeSMTP:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            calls["host"] = host
            calls["port"] = port
            calls["timeout"] = timeout

        def __enter__(self) -> _FakeSMTP:
            return self

        def __exit__(self, *_args) -> None:
            return None

        def ehlo(self) -> None:
            calls["ehlo"] = int(calls.get("ehlo", 0)) + 1

        def starttls(self, *, context) -> None:
            calls["starttls"] = context is not None

        def login(self, username: str, password: str) -> None:
            calls["login"] = (username, password)

        def send_message(self, message) -> None:
            calls["subject"] = message["Subject"]
            calls["to"] = message["To"]
            calls["body"] = message.get_content()

    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)

    delivered = notifier.send_lines("**Morning Check**", "Trader is live.")

    assert delivered is True
    assert calls["host"] == "smtp.gmail.com"
    assert calls["port"] == 587
    assert calls["login"] == ("sender@example.com", "app-password")
    assert calls["subject"] == "[CodexAlpaca] Morning Check"
    assert calls["to"] == "dest@example.com"
    assert "Trader is live." in str(calls["body"])
