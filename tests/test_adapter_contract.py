from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.adapters.base import HttpPaperAdapter, RetryConfig
from src.digest_pipeline import build_default_adapters, load_sources_config
from src.schemas import PaperRecord


class DummyAdapter(HttpPaperAdapter):
    def required_headers(self) -> list[str]:
        return ["X-Test-Key"]

    def build_query_params(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
    ) -> dict[str, Any]:
        return {"q": query, "cursor": cursor or "0"}

    def parse_records(self, payload: dict[str, Any]) -> list[PaperRecord]:
        rows = payload.get("items", [])
        records = []
        for row in rows:
            records.append(
                PaperRecord(
                    title=str(row.get("title") or ""),
                    source="dummy",
                )
            )
        return records


class AdapterContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.from_ts = datetime.now() - timedelta(days=1)
        self.to_ts = datetime.now()

    def test_default_adapters_skip_without_auth(self) -> None:
        config = load_sources_config(Path("config") / "sources.yaml")
        adapters = build_default_adapters(config)

        for adapter in adapters.values():
            result = adapter.fetch_incremental(
                query="train scheduling",
                from_ts=self.from_ts,
                to_ts=self.to_ts,
                cursor=None,
                auth_ctx={},
            )
            self.assertEqual(result.status, "skipped")
            self.assertEqual(result.reason, "auth_missing")

    def test_auth_forbidden_returns_error(self) -> None:
        adapter = DummyAdapter(
            source_name="dummy",
            base_url="https://example.com",
            auth_mode="api_key",
            retry_config=RetryConfig(timeout_seconds=1, max_retries=2, backoff_seconds=0),
            http_executor=lambda *_: (401, {}),
            sleep_fn=lambda _: None,
        )
        result = adapter.fetch_incremental(
            query="q",
            from_ts=self.from_ts,
            to_ts=self.to_ts,
            cursor=None,
            auth_ctx={"X-Test-Key": "k"},
        )
        self.assertEqual(result.status, "error")
        self.assertEqual(result.reason, "auth_forbidden")
        self.assertIn("401", result.alert or "")

    def test_retry_then_success(self) -> None:
        responses = iter(
            [
                (429, {}),
                (200, {"items": [{"title": "A"}]}),
            ]
        )

        def executor(*_: Any) -> tuple[int, dict[str, Any]]:
            return next(responses)

        adapter = DummyAdapter(
            source_name="dummy",
            base_url="https://example.com",
            auth_mode="api_key",
            retry_config=RetryConfig(timeout_seconds=1, max_retries=2, backoff_seconds=0),
            http_executor=executor,
            sleep_fn=lambda _: None,
        )
        result = adapter.fetch_incremental(
            query="q",
            from_ts=self.from_ts,
            to_ts=self.to_ts,
            cursor=None,
            auth_ctx={"X-Test-Key": "k"},
        )
        self.assertEqual(result.status, "ok")
        self.assertEqual(len(result.records), 1)

    def test_retry_exhausted_for_5xx(self) -> None:
        adapter = DummyAdapter(
            source_name="dummy",
            base_url="https://example.com",
            auth_mode="api_key",
            retry_config=RetryConfig(timeout_seconds=1, max_retries=1, backoff_seconds=0),
            http_executor=lambda *_: (503, {}),
            sleep_fn=lambda _: None,
        )
        result = adapter.fetch_incremental(
            query="q",
            from_ts=self.from_ts,
            to_ts=self.to_ts,
            cursor=None,
            auth_ctx={"X-Test-Key": "k"},
        )
        self.assertEqual(result.status, "error")
        self.assertEqual(result.reason, "retry_exhausted")


if __name__ == "__main__":
    unittest.main()

