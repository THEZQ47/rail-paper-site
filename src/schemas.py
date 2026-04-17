from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

SourceStatus = Literal["ok", "skipped", "error"]


@dataclass(frozen=True)
class PaperRecord:
    title: str
    authors: tuple[str, ...] = ()
    venue: str = ""
    year: int | None = None
    abstract: str = ""
    url: str = ""
    source: str = ""
    doi: str | None = None
    keywords: tuple[str, ...] = ()
    published_at: datetime | None = None


@dataclass(frozen=True)
class SourceResult:
    source: str
    status: SourceStatus
    reason: str | None = None
    records: tuple[PaperRecord, ...] = ()
    next_cursor: str | None = None
    alert: str | None = None


@dataclass(frozen=True)
class DigestCard:
    rank: int
    title: str
    source: str
    year: int | None
    url: str
    relevance_score: int
    exact_method_tags: tuple[str, ...] = ()
    problem_tags: tuple[str, ...] = ()
    one_line_takeaway: str = ""


@dataclass(frozen=True)
class DigestReport:
    generated_at: datetime
    from_ts: datetime
    to_ts: datetime
    cards: tuple[DigestCard, ...] = ()
    source_results: tuple[SourceResult, ...] = ()
    total_raw_records: int = 0
    total_unique_records: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

