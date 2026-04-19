from __future__ import annotations

import threading
import time

import requests

from app.core.config import settings


class KeepAliveService:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        if not settings.enable_keep_alive:
            return
        with self._lock:
            if self._started:
                return
            self._thread = threading.Thread(target=self._run, name="render-keep-alive", daemon=True)
            self._thread.start()
            self._started = True

    def _run(self) -> None:
        interval = max(int(settings.keep_alive_interval_seconds), 300)
        url = settings.keep_alive_url.strip()
        if not url:
            return

        while True:
            try:
                requests.get(url, timeout=10)
            except requests.RequestException:
                pass
            time.sleep(interval)


keep_alive_service = KeepAliveService()
