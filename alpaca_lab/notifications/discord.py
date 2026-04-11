from __future__ import annotations

from typing import Any

import requests

from alpaca_lab.config import LabSettings
from alpaca_lab.logging_utils import get_logger


class DiscordWebhookNotifier:
    def __init__(self, settings: LabSettings, *, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.logger = get_logger("discord")

    @property
    def enabled(self) -> bool:
        return self.settings.discord_webhook_url is not None

    def send(self, content: str) -> bool:
        if not self.enabled:
            return False
        webhook = self.settings.discord_webhook_url
        if webhook is None:
            return False
        try:
            response = self.session.post(
                webhook.get_secret_value(),
                json={"content": content[:1900]},
                timeout=self.settings.request_timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            self.logger.warning("discord webhook failed: %s", exc)
            return False
        return True

    def send_lines(self, *lines: Any) -> bool:
        content = "\n".join(str(line) for line in lines if str(line).strip())
        return self.send(content)
