from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.adapters.base import HttpPaperAdapter, RetryConfig
from src.schemas import PaperRecord


class WosAdapter(HttpPaperAdapter):
    def __init__(
        self,
        base_url: str,
        auth_mode: str,
        retry_config: RetryConfig,
        page_size: int = 25,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            source_name="wos",
            base_url=base_url,
            auth_mode=auth_mode,
            retry_config=retry_config,
            page_size=page_size,
            **kwargs,
        )

    def required_headers(self) -> list[str]:
        return ["X-ApiKey"]

    def build_query_params(
        self,
        query: str,
        from_ts: datetime,
        to_ts: datetime,
        cursor: str | None,
    ) -> dict[str, Any]:
        page = int(cursor) if cursor else 1
        return {
            "db": "WOS",
            "q": query,
            "limit": self.page_size,
            "page": page,
            "sortField": "LD+D",
            "fromDate": from_ts.strftime("%Y-%m-%d"),
            "toDate": to_ts.strftime("%Y-%m-%d"),
        }

    def parse_records(self, payload: Mapping[str, Any]) -> list[PaperRecord]:
        rows = payload.get("hits") or payload.get("records") or []
        records: list[PaperRecord] = []
        for row in rows:
            title = str(row.get("title") or row.get("documentTitle") or "").strip()
            if not title:
                continue
            authors = row.get("authors") or row.get("names") or []
            records.append(
                PaperRecord(
                    title=title,
                    authors=tuple(str(x) for x in authors if str(x).strip()),
                    venue=str(row.get("sourceTitle") or row.get("source") or ""),
                    year=_safe_int(row.get("publishYear") or row.get("year")),
                    abstract=str(row.get("abstract") or ""),
                    url=str(row.get("recordLink") or row.get("url") or ""),
                    source="wos",
                    doi=_normalize_doi(row.get("doi")),
                )
            )
        return records

    def parse_next_cursor(self, payload: Mapping[str, Any], cursor: str | None) -> str | None:
        page = int(cursor) if cursor else 1
        total = _safe_int(payload.get("metadata", {}).get("total")) or _safe_int(payload.get("total"))
        if total is None:
            return None
        if page * self.page_size >= total:
            return None
        return str(page + 1)


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

