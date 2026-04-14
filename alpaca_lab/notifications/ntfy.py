from __future__ import annotations

from typing import Any
from urllib.parse import quote

import requests

from alpaca_lab.config import LabSettings
from alpaca_lab.logging_utils import get_logger


class NtfyNotifier:
    def __init__(self, settings: LabSettings, *, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.logger = get_logger("ntfy")

    @property
    def enabled(self) -> bool:
        return self.settings.ntfy_topic is not None

    def _topic_url(self) -> str | None:
        topic = self.settings.ntfy_topic
        if topic is None:
            return None
        return f"{self.settings.ntfy_base_url.rstrip('/')}/{quote(topic, safe='/')}"

    def _build_headers(self, content: str) -> dict[str, str]:
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
        }
        title = next((line.strip() for line in content.splitlines() if line.strip()), "CodexAlpaca")
        headers["Title"] = title.strip("*").strip()[:120] or "CodexAlpaca"
        token = self.settings.ntfy_access_token
        if token is not None:
            headers["Authorization"] = f"Bearer {token.get_secret_value()}"
        return headers

    def send(self, content: str) -> bool:
        topic_url = self._topic_url()
        if topic_url is None:
            return False
        try:
            response = self.session.post(
                topic_url,
                data=content[:10000].encode("utf-8"),
                headers=self._build_headers(content),
                timeout=self.settings.request_timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            self.logger.warning("ntfy notification failed: %s", exc)
            return False
        return True

    def send_lines(self, *lines: Any) -> bool:
        content = "\n".join(str(line) for line in lines if str(line).strip())
        if not content:
            return False
        return self.send(content)
