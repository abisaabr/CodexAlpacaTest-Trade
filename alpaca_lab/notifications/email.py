from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage
from typing import Any

from alpaca_lab.config import LabSettings
from alpaca_lab.logging_utils import get_logger


class EmailNotifier:
    def __init__(self, settings: LabSettings) -> None:
        self.settings = settings
        self.logger = get_logger("email")

    @property
    def enabled(self) -> bool:
        return bool(
            self.settings.email_smtp_host
            and self.settings.email_from
            and self.settings.email_to
            and self.settings.email_username
            and self.settings.email_password
        )

    def _build_subject(self, content: str) -> str:
        first_line = next((line.strip() for line in content.splitlines() if line.strip()), "Notification")
        subject_line = first_line.strip("*").strip() or "Notification"
        prefix = self.settings.email_subject_prefix.strip()
        if prefix:
            return f"{prefix} {subject_line}"
        return subject_line

    def send(self, content: str) -> bool:
        if not self.enabled:
            return False
        host = self.settings.email_smtp_host
        sender = self.settings.email_from
        username = self.settings.email_username
        password = self.settings.email_password
        if host is None or sender is None or username is None or password is None:
            return False

        message = EmailMessage()
        message["Subject"] = self._build_subject(content)
        message["From"] = sender
        message["To"] = ", ".join(self.settings.email_to)
        message.set_content(content[:10000])

        try:
            with smtplib.SMTP(
                host,
                self.settings.email_smtp_port,
                timeout=self.settings.request_timeout_seconds,
            ) as smtp:
                smtp.ehlo()
                if self.settings.email_use_starttls:
                    smtp.starttls(context=ssl.create_default_context())
                    smtp.ehlo()
                smtp.login(username.get_secret_value(), password.get_secret_value())
                smtp.send_message(message)
        except (OSError, smtplib.SMTPException) as exc:
            self.logger.warning("email notification failed: %s", exc)
            return False
        return True

    def send_lines(self, *lines: Any) -> bool:
        content = "\n".join(str(line) for line in lines if str(line).strip())
        if not content:
            return False
        return self.send(content)
