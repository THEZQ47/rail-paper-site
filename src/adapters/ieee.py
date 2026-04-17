from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.adapters.base import HttpPaperAdapter, RetryConfig
from src.schemas import PaperRecord


class IeeeAdapter(HttpPaperAdapter):
    def __init__(
        self,
        base_url: str,
        auth_mode: str,
        retry_config: RetryConfig,
        page_size: int = 25,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            source_name="ieee",
            base_url=base_url,
            auth_mode=auth_mode,
            retry_config=retry_config,
            page_size=page_size,
            **kwargs,
        )

    def required_headers(self) -> list[str]:
        return ["x-api-key"]

    def build_query_params(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
    ) -> dict[str, Any]:
        start = int(cursor) if cursor else 1
        return {
            "querytext": query,
            "max_records": self.page_size,
            "start_record": start,
            "start_year": from_ts.year,
            "end_year": to_ts.year,
            "sort_order": "desc",
            "sort_field": "article_number",
            "format": "json",
        }

    def parse_records(self, payload: Mapping[str, Any]) -> list[PaperRecord]:
        rows = payload.get("articles", [])
        records: list[PaperRecord] = []
        for row in rows:
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            authors = _extract_authors(row.get("authors"))
            records.append(
                PaperRecord(
                    title=title,
                    authors=tuple(authors),
                    venue=str(row.get("publication_title") or ""),
                    year=_safe_int(row.get("publication_year")),
                    abstract=str(row.get("abstract") or ""),
                    url=str(row.get("html_url") or row.get("pdf_url") or ""),
                    source="ieee",
                    doi=_normalize_doi(row.get("doi")),
                )
            )
        return records

    def parse_next_cursor(self, payload: Mapping[str, Any], cursor: str | None) -> str | None:
        start = int(cursor) if cursor else 1
        total = _safe_int(payload.get("total_records"))
        if total is None:
            return None
        if start - 1 + self.page_size >= total:
            return None
        return str(start + self.page_size)


def _extract_authors(authors_node: Any) -> list[str]:
    if isinstance(authors_node, Mapping):
        entries = authors_node.get("authors")
        if isinstance(entries, list):
            names: list[str] = []
            for entry in entries:
                if isinstance(entry, Mapping) and entry.get("full_name"):
                    names.append(str(entry["full_name"]))
            return names
    return []


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_doi(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None

