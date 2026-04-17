from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.adapters.base import HttpPaperAdapter, RetryConfig
from src.schemas import PaperRecord


class ScopusAdapter(HttpPaperAdapter):
    def __init__(
        self,
        base_url: str,
        auth_mode: str,
        retry_config: RetryConfig,
        page_size: int = 25,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            source_name="scopus",
            base_url=base_url,
            auth_mode=auth_mode,
            retry_config=retry_config,
            page_size=page_size,
            **kwargs,
        )

    def required_headers(self) -> list[str]:
        # Elsevier 常见接入需要 API Key + 机构 token。
        return ["X-ELS-APIKey", "X-ELS-Insttoken"]

    def build_query_params(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
    ) -> dict[str, Any]:
        start = int(cursor) if cursor else 0
        date_range = f"{from_ts.year}-{to_ts.year}"
        return {
            "query": f"{query} AND PUBYEAR AFT {from_ts.year - 1} AND PUBYEAR BEF {to_ts.year + 1}",
            "count": self.page_size,
            "start": start,
            "date": date_range,
            "view": "COMPLETE",
        }

    def parse_records(self, payload: Mapping[str, Any]) -> list[PaperRecord]:
        root = payload.get("search-results", {})
        rows = root.get("entry", [])
        records: list[PaperRecord] = []
        for row in rows:
            title = str(row.get("dc:title") or "").strip()
            if not title:
                continue
            authors = _split_authors(row.get("dc:creator"))
            records.append(
                PaperRecord(
                    title=title,
                    authors=tuple(authors),
                    venue=str(row.get("prism:publicationName") or ""),
                    year=_parse_year(row.get("prism:coverDate")),
                    abstract=str(row.get("dc:description") or ""),
                    url=_extract_url(row),
                    source="scopus",
                    doi=_normalize_doi(row.get("prism:doi")),
                )
            )
        return records

    def parse_next_cursor(self, payload: Mapping[str, Any], cursor: str | None) -> str | None:
        start = int(cursor) if cursor else 0
        root = payload.get("search-results", {})
        total = _safe_int(root.get("opensearch:totalResults"))
        if total is None:
            return None
        if start + self.page_size >= total:
            return None
        return str(start + self.page_size)


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_year(cover_date: Any) -> int | None:
    if cover_date is None:
        return None
    text = str(cover_date)
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def _split_authors(creator: Any) -> list[str]:
    if creator is None:
        return []
    text = str(creator).strip()
    if not text:
        return []
    if "," in text:
        return [x.strip() for x in text.split(",") if x.strip()]
    return [text]


def _extract_url(row: Mapping[str, Any]) -> str:
    links = row.get("link")
    if isinstance(links, list):
        for entry in links:
            if isinstance(entry, Mapping) and entry.get("@href"):
                return str(entry["@href"])
    return str(row.get("prism:url") or "")


def _normalize_doi(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None

