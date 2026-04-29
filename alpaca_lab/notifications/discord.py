from __future__ import annotations

import json
import os
import ssl
import subprocess
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

try:
    import truststore
except ImportError:  # pragma: no cover - optional runtime hardening
    truststore = None

from alpaca_lab.config import LabSettings
from alpaca_lab.logging_utils import get_logger


def _build_ssl_context() -> ssl.SSLContext:
    if truststore is not None:
        context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    else:
        context = ssl.create_default_context()
    minimum_tls = getattr(ssl.TLSVersion, "TLSv1_2", None)
    if minimum_tls is not None:
        context.minimum_version = minimum_tls
    return context


class _TlsHttpAdapter(HTTPAdapter):
    def __init__(self, *, ssl_context: ssl.SSLContext) -> None:
        self._ssl_context = ssl_context
        super().__init__()

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **pool_kwargs: Any,
    ) -> None:
        pool_kwargs["ssl_context"] = self._ssl_context
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            **pool_kwargs,
        )

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
        proxy_kwargs["ssl_context"] = self._ssl_context
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def _build_discord_session() -> requests.Session:
    session = requests.Session()
    session.mount("https://", _TlsHttpAdapter(ssl_context=_build_ssl_context()))
    return session


class DiscordWebhookNotifier:
    def __init__(self, settings: LabSettings, *, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or _build_discord_session()
        self.logger = get_logger("discord")

    @property
    def enabled(self) -> bool:
        return self.settings.discord_webhook_url is not None

    def _send_via_powershell(self, webhook_url: str, content: str) -> str | None:
        if os.name != "nt":
            return "PowerShell fallback is only available on Windows."
        payload = json.dumps({"content": content[:1900]})
        script = (
            "$ProgressPreference='SilentlyContinue'; "
            "try { "
            "[Net.ServicePointManager]::SecurityProtocol = "
            "[Net.SecurityProtocolType]::Tls12 -bor [enum]::Parse([Net.SecurityProtocolType], 'Tls13') "
            "} catch { "
            "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 "
            "}; "
            "Invoke-RestMethod -Method Post -Uri $env:CODEX_DISCORD_WEBHOOK_URL "
            "-ContentType 'application/json' -Body $env:CODEX_DISCORD_PAYLOAD | Out-Null"
        )
        env = os.environ.copy()
        env["CODEX_DISCORD_WEBHOOK_URL"] = webhook_url
        env["CODEX_DISCORD_PAYLOAD"] = payload
        try:
            completed = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    script,
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=self.settings.request_timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return str(exc)
        if completed.returncode == 0:
            return None
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        return stderr or stdout or f"powershell exited with code {completed.returncode}"

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
            fallback_error = self._send_via_powershell(webhook.get_secret_value(), content)
            if fallback_error is None:
                self.logger.info("discord webhook delivered via PowerShell fallback")
                return True
            self.logger.warning("discord PowerShell fallback failed: %s", fallback_error)
            return False
        return True

    def send_lines(self, *lines: Any) -> bool:
        content = "\n".join(str(line) for line in lines if str(line).strip())
        return self.send(content)
