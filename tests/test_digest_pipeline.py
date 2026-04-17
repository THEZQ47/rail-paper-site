from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from typing import Mapping

from src.adapters.base import PaperSourceAdapter
from src.digest_pipeline import render_report_markdown, run_daily_digest
from src.schemas import PaperRecord, SourceResult


class StaticAdapter(PaperSourceAdapter):
    def __init__(self, source_name: str, result: SourceResult) -> None:
        self.source_name = source_name
        self._result = result

    def fetch_incremental(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
        auth_ctx: Mapping[str, str],
    ) -> SourceResult:
        return self._result


class StaticAuthProvider:
    def get_headers(self, source_name: str) -> dict[str, str]:
        return {"Authorization": "dummy"}


class DigestPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.from_ts = datetime.now() - timedelta(days=1)
        self.to_ts = datetime.now()
        self.config = {
            "sources": {
                "wos": {"enabled": True},
                "scopus": {"enabled": True},
            }
        }

    def test_deduplicate_and_rank(self) -> None:
        wos_records = (
            PaperRecord(
                title="Train rescheduling with branch-and-bound under disruptions",
                source="wos",
                year=2025,
                doi="10.1000/abc",
                abstract="railway timetable integer programming",
                url="https://example.org/a",
            ),
            PaperRecord(
                title="Train scheduling with integer programming",
                source="wos",
                year=2024,
                doi="10.1000/xyz",
                abstract="rail transit dispatching",
                url="https://example.org/b",
            ),
        )
        scopus_records = (
            PaperRecord(
                title="Train rescheduling with branch-and-bound under disruptions",
                source="scopus",
                year=2025,
                doi="10.1000/abc",
                abstract="duplicate by doi",
                url="https://example.org/c",
            ),
            PaperRecord(
                title="Metaheuristic review for airport stand assignment",
                source="scopus",
                year=2022,
                doi="10.1000/other",
                abstract="not rail scope",
                url="https://example.org/d",
            ),
        )
        adapters = {
            "wos": StaticAdapter(
                "wos",
                SourceResult(source="wos", status="ok", records=wos_records),
            ),
            "scopus": StaticAdapter(
                "scopus",
                SourceResult(source="scopus", status="ok", records=scopus_records),
            ),
        }

        report = run_daily_digest(
            config=self.config,
            query="train scheduling",
            from_ts=self.from_ts,
            to_ts=self.to_ts,
            max_items=10,
            adapters=adapters,
            auth_provider=StaticAuthProvider(),
        )

        self.assertEqual(report.total_raw_records, 4)
        self.assertEqual(report.total_unique_records, 3)
        self.assertLessEqual(len(report.cards), 10)
        self.assertIn("branch-and-bound", report.cards[0].title.lower())
        self.assertGreaterEqual(report.cards[0].relevance_score, report.cards[-1].relevance_score)

    def test_empty_cards_still_render_report(self) -> None:
        adapters = {
            "wos": StaticAdapter("wos", SourceResult(source="wos", status="skipped", reason="auth_missing")),
            "scopus": StaticAdapter("scopus", SourceResult(source="scopus", status="skipped", reason="auth_missing")),
        }

        report = run_daily_digest(
            config=self.config,
            query="train scheduling",
            from_ts=self.from_ts,
            to_ts=self.to_ts,
            adapters=adapters,
            auth_provider=StaticAuthProvider(),
        )
        markdown = render_report_markdown(report)

        self.assertEqual(len(report.cards), 0)
        self.assertIn("未取到可用论文", markdown)
        self.assertIn("auth_missing", markdown)


if __name__ == "__main__":
    unittest.main()

