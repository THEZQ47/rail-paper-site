from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.schemas import PaperRecord, SourceResult

HttpExecutor = Callable[[str, Mapping[str, str], Mapping[str, Any], int], tuple[int, dict[str, Any]]]


@dataclass(frozen=True)
class RetryConfig:
    timeout_seconds: int
    max_retries: int
    backoff_seconds: float


class PaperSourceAdapter(ABC):
    source_name: str

    @abstractmethod
    def fetch_incremental(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
        auth_ctx: Mapping[str, str],
    ) -> SourceResult:
        raise NotImplementedError


def default_http_get_json(
    base_url: str,
    headers: Mapping[str, str],
    params: Mapping[str, Any],
    timeout_seconds: int,
) -> tuple[int, dict[str, Any]]:
    query = urlencode(params, doseq=True)
    target_url = f"{base_url}?{query}" if query else base_url
    request = Request(target_url, headers=dict(headers), method="GET")
    raw_text = ""
    status_code = 0

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.getcode() or 200
            raw_text = response.read().decode("utf-8")
    except HTTPError as exc:
        status_code = exc.code
        if exc.fp:
            raw_text = exc.read().decode("utf-8")
    except URLError as exc:
        raise ConnectionError(str(exc)) from exc

    if not raw_text.strip():
        return status_code, {}
    payload = json.loads(raw_text)
    if isinstance(payload, dict):
        return status_code, payload
    return status_code, {"items": payload}


class HttpPaperAdapter(PaperSourceAdapter, ABC):
    def __init__(
        self,
        source_name: str,
        base_url: str,
        auth_mode: str,
        retry_config: RetryConfig,
        page_size: int = 25,
        http_executor: HttpExecutor = default_http_get_json,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.source_name = source_name
        self.base_url = base_url
        self.auth_mode = auth_mode
        self.retry_config = retry_config
        self.page_size = page_size
        self.http_executor = http_executor
        self.sleep_fn = sleep_fn

    def fetch_incremental(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
        auth_ctx: Mapping[str, str],
    ) -> SourceResult:
        required_headers = self.required_headers()
        if self.auth_mode != "none":
            missing = [header for header in required_headers if not auth_ctx.get(header)]
            if missing:
                return SourceResult(
                    source=self.source_name,
                    status="skipped",
                    reason="auth_missing",
                    alert=f"missing_headers={','.join(sorted(missing))}",
                )

        params = self.build_query_params(query=query, from_ts=from_ts, to_ts=to_ts, cursor=cursor)
        request_result = self._request_with_retry(auth_ctx=auth_ctx, params=params)
        if request_result.status == "error":
            return SourceResult(
                source=self.source_name,
                status="error",
                reason=request_result.reason,
                alert=request_result.alert,
            )

        records = self.parse_records(request_result.payload or {})
        return SourceResult(
            source=self.source_name,
            status="ok",
            records=tuple(records),
            next_cursor=self.parse_next_cursor(request_result.payload or {}, cursor),
        )

    @dataclass(frozen=True)
    class _RequestResult:
        status: str
        reason: str | None = None
        payload: dict[str, Any] | None = None
        alert: str | None = None

    def _request_with_retry(
        self,
        auth_ctx: Mapping[str, str],
        params: Mapping[str, Any],
    ) -> _RequestResult:
        max_retries = max(self.retry_config.max_retries, 0)
        backoff_seconds = max(self.retry_config.backoff_seconds, 0.0)
        timeout_seconds = max(self.retry_config.timeout_seconds, 1)

        for attempt in range(max_retries + 1):
            try:
                status_code, payload = self.http_executor(
                    self.base_url,
                    auth_ctx,
                    params,
                    timeout_seconds,
                )
            except (ConnectionError, TimeoutError, json.JSONDecodeError):
                status_code = 599
                payload = {}

            if status_code in (401, 403):
                return self._RequestResult(
                    status="error",
                    reason="auth_forbidden",
                    alert=f"{self.source_name} returned {status_code}",
                )

            if status_code == 429 or 500 <= status_code < 600:
                if attempt == max_retries:
                    return self._RequestResult(
                        status="error",
                        reason="retry_exhausted",
                        alert=f"{self.source_name} returned {status_code} after retries",
                    )
                self.sleep_fn(backoff_seconds * (2**attempt))
                continue

            if status_code >= 400:
                return self._RequestResult(
                    status="error",
                    reason=f"http_{status_code}",
                    alert=f"{self.source_name} returned {status_code}",
                )

            return self._RequestResult(status="ok", payload=payload)

        return self._RequestResult(status="error", reason="unreachable")

    def required_headers(self) -> list[str]:
        return []

    @abstractmethod
    def build_query_params(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def parse_records(self, payload: Mapping[str, Any]) -> list[PaperRecord]:
        raise NotImplementedError

    def parse_next_cursor(self, payload: Mapping[str, Any], cursor: str | None) -> str | None:
        return None

