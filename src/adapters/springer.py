from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.adapters.base import HttpPaperAdapter, RetryConfig
from src.schemas import PaperRecord


class SpringerAdapter(HttpPaperAdapter):
    def __init__(
        self,
        base_url: str,
        auth_mode: str,
        retry_config: RetryConfig,
        page_size: int = 25,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            source_name="springer",
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
        start = int(cursor) if cursor else 1
        date_clause = (
            f"year:{from_ts.year} OR year:{from_ts.year + 1} OR "
            f"year:{to_ts.year - 1} OR year:{to_ts.year}"
        )
        return {
            "q": f"({query}) AND ({date_clause})",
            "p": self.page_size,
            "s": start,
        }

    def parse_records(self, payload: Mapping[str, Any]) -> list[PaperRecord]:
        rows = payload.get("records", [])
        records: list[PaperRecord] = []
        for row in rows:
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            creators = row.get("creators")
            records.append(
                PaperRecord(
                    title=title,
                    authors=tuple(_parse_creators(creators)),
                    venue=str(row.get("publicationName") or ""),
                    year=_safe_int(row.get("publicationDate", "")[:4]),
                    abstract=str(row.get("abstract") or ""),
                    url=_extract_url(row),
                    source="springer",
                    doi=_normalize_doi(row.get("doi")),
                )
            )
        return records

    def parse_next_cursor(self, payload: Mapping[str, Any], cursor: str | None) -> str | None:
        start = int(cursor) if cursor else 1
        total = _safe_int(payload.get("result", [{}])[0].get("total")) if payload.get("result") else None
        if total is None:
            return None
        if (start - 1) + self.page_size >= total:
            return None
        return str(start + self.page_size)


def _parse_creators(creators: Any) -> list[str]:
    if isinstance(creators, list):
        return [str(item.get("creator")) for item in creators if isinstance(item, Mapping) and item.get("creator")]
    if isinstance(creators, str):
        parts = [x.strip() for x in creators.split(";") if x.strip()]
        return parts
    return []


def _extract_url(row: Mapping[str, Any]) -> str:
    urls = row.get("url")
    if isinstance(urls, list):
        for entry in urls:
            if isinstance(entry, Mapping) and entry.get("value"):
                return str(entry["value"])
    return ""


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

