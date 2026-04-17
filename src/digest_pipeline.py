from __future__ import annotations

import argparse
import json
import re
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.adapters import IeeeAdapter, PaperSourceAdapter, ScopusAdapter, SpringerAdapter, WosAdapter
from src.adapters.base import RetryConfig
from src.auth import SourceAuthProvider
from src.schemas import DigestCard, DigestReport, PaperRecord, SourceResult

DEFAULT_QUERY = (
    "\"train scheduling\" OR \"train rescheduling\" OR \"railway timetable\" "
    "OR \"rail transit dispatching\""
)

RAIL_SCOPE_TERMS = [
    "train scheduling",
    "train rescheduling",
    "railway timetable",
    "rail transit dispatching",
    "railway",
    "列车调度",
    "列车重调度",
    "运行图",
]

EXACT_METHOD_KEYWORDS = {
    "branch and bound": "B&B",
    "branch-and-bound": "B&B",
    "benders": "Benders",
    "dynamic programming": "DP",
    "lagrangian": "Lagrangian",
    "integer programming": "IP",
    "mixed integer": "MIP",
    "column generation": "Column Generation",
    "branch-and-price": "Branch-and-Price",
}

PROBLEM_TAG_KEYWORDS = {
    "运行图": ["timetable", "运行图"],
    "重调度": ["rescheduling", "重调度", "disruption"],
    "冲突消解": ["conflict", "headway", "冲突"],
    "网络级调度": ["network", "corridor", "网络"],
}


def load_sources_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    return json.loads(path.read_text(encoding="utf-8"))


def build_default_adapters(config: Mapping[str, Any]) -> dict[str, PaperSourceAdapter]:
    defaults = config.get("defaults", {})
    sources_cfg = config.get("sources", {})
    adapters: dict[str, PaperSourceAdapter] = {}

    for source_name, source_cfg in sources_cfg.items():
        retry_cfg = source_cfg.get("retry", {})
        retry = RetryConfig(
            timeout_seconds=int(source_cfg.get("timeout_seconds", defaults.get("timeout_seconds", 15))),
            max_retries=int(retry_cfg.get("max_retries", defaults.get("max_retries", 3))),
            backoff_seconds=float(retry_cfg.get("backoff_seconds", defaults.get("backoff_seconds", 1.0))),
        )
        page_size = int(source_cfg.get("page_size", defaults.get("page_size", 25)))
        base_url = str(source_cfg.get("base_url", ""))
        auth_mode = str(source_cfg.get("auth_mode", "none"))
        kwargs = {"base_url": base_url, "auth_mode": auth_mode, "retry_config": retry, "page_size": page_size}

        if source_name == "wos":
            adapters[source_name] = WosAdapter(**kwargs)
        elif source_name == "scopus":
            adapters[source_name] = ScopusAdapter(**kwargs)
        elif source_name == "ieee":
            adapters[source_name] = IeeeAdapter(**kwargs)
        elif source_name == "springer":
            adapters[source_name] = SpringerAdapter(**kwargs)
    return adapters


def run_daily_digest(
    config: Mapping[str, Any],
    query: str,
    from_ts: datetime,
    to_ts: datetime,
    max_items: int = 10,
    adapters: Mapping[str, PaperSourceAdapter] | None = None,
    auth_provider: SourceAuthProvider | None = None,
) -> DigestReport:
    sources_cfg = config.get("sources", {})
    adapters = dict(adapters) if adapters is not None else build_default_adapters(config)
    auth_provider = auth_provider or SourceAuthProvider(sources_cfg)

    source_results: list[SourceResult] = []
    all_records: list[PaperRecord] = []

    for source_name, source_cfg in sources_cfg.items():
        if not bool(source_cfg.get("enabled", False)):
            source_results.append(
                SourceResult(source=source_name, status="skipped", reason="disabled"),
            )
            continue

        adapter = adapters.get(source_name)
        if adapter is None:
            source_results.append(
                SourceResult(source=source_name, status="error", reason="adapter_missing"),
            )
            continue

        headers = auth_provider.get_headers(source_name)
        result = adapter.fetch_incremental(
            query=query,
            from_ts=from_ts,
            to_ts=to_ts,
            cursor=None,
            auth_ctx=headers,
        )
        source_results.append(result)
        if result.status == "ok":
            all_records.extend(result.records)

    unique_records = deduplicate_records(all_records)
    cards = build_digest_cards(unique_records, max_items=max_items)
    notes: list[str] = []
    if not cards:
        notes.append("今日未从已启用数据源提取到可用论文，请检查凭据或扩大检索窗口。")

    return DigestReport(
        generated_at=datetime.now(),
        from_ts=from_ts,
        to_ts=to_ts,
        cards=tuple(cards),
        source_results=tuple(source_results),
        total_raw_records=len(all_records),
        total_unique_records=len(unique_records),
        notes=tuple(notes),
    )


def run_daily_digest_from_file(
    config_path: str | Path,
    query: str = DEFAULT_QUERY,
    lookback_hours: int = 24,
    max_items: int = 10,
) -> DigestReport:
    config = load_sources_config(config_path)
    now = datetime.now()
    from_ts = now - timedelta(hours=lookback_hours)
    return run_daily_digest(
        config=config,
        query=query,
        from_ts=from_ts,
        to_ts=now,
        max_items=max_items,
    )


def deduplicate_records(records: Iterable[PaperRecord]) -> list[PaperRecord]:
    seen_doi: set[str] = set()
    seen_title_year: set[tuple[str, int | None]] = set()
    unique: list[PaperRecord] = []

    for record in records:
        doi = (record.doi or "").strip().lower()
        if doi:
            if doi in seen_doi:
                continue
            seen_doi.add(doi)

        key = (_normalize_title(record.title), record.year)
        if key in seen_title_year:
            continue
        seen_title_year.add(key)
        unique.append(record)
    return unique


def build_digest_cards(records: Iterable[PaperRecord], max_items: int = 10) -> list[DigestCard]:
    scored = []
    for record in records:
        score, method_tags, problem_tags = score_record(record)
        scored.append((score, record, method_tags, problem_tags))

    scored.sort(
        key=lambda item: (
            item[0],
            item[1].year or 0,
            item[1].title.lower(),
        ),
        reverse=True,
    )

    cards: list[DigestCard] = []
    for rank, (score, record, method_tags, problem_tags) in enumerate(scored[:max_items], start=1):
        cards.append(
            DigestCard(
                rank=rank,
                title=record.title,
                source=record.source,
                year=record.year,
                url=record.url,
                relevance_score=score,
                exact_method_tags=tuple(method_tags),
                problem_tags=tuple(problem_tags),
                one_line_takeaway=_make_takeaway(method_tags, problem_tags),
            )
        )
    return cards


def score_record(record: PaperRecord) -> tuple[int, list[str], list[str]]:
    text = " ".join(
        [
            record.title,
            record.abstract,
            " ".join(record.keywords),
            record.venue,
        ]
    ).lower()

    score = 0
    method_tags: set[str] = set()
    problem_tags: set[str] = set()

    for term in RAIL_SCOPE_TERMS:
        if term in text:
            score += 5

    for term, tag in EXACT_METHOD_KEYWORDS.items():
        if term in text:
            score += 4
            method_tags.add(tag)

    for tag, terms in PROBLEM_TAG_KEYWORDS.items():
        if any(term in text for term in terms):
            score += 2
            problem_tags.add(tag)

    # 严格轨道调度范围：完全无轨道调度词时降低优先级。
    if not any(term in text for term in RAIL_SCOPE_TERMS):
        score -= 8

    return score, sorted(method_tags), sorted(problem_tags)


def render_report_markdown(report: DigestReport) -> str:
    lines: list[str] = []
    lines.append("# 轨道交通列车调度文献日报")
    lines.append("")
    lines.append(f"- 生成时间: {report.generated_at.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(
        f"- 检索窗口: {report.from_ts.strftime('%Y-%m-%d %H:%M')} ~ {report.to_ts.strftime('%Y-%m-%d %H:%M')}"
    )
    lines.append(f"- 原始记录数: {report.total_raw_records}")
    lines.append(f"- 去重后记录数: {report.total_unique_records}")
    lines.append("")
    lines.append("## 数据源状态")
    for result in report.source_results:
        note = f" ({result.reason})" if result.reason else ""
        alert = f" | {result.alert}" if result.alert else ""
        lines.append(f"- {result.source}: {result.status}{note}{alert}")

    if report.cards:
        lines.append("")
        lines.append("## 今日 Top 论文")
        for card in report.cards:
            lines.append("")
            lines.append(f"### {card.rank}. {card.title}")
            lines.append(f"- 来源: {card.source}")
            lines.append(f"- 年份: {card.year if card.year is not None else '未知'}")
            lines.append(f"- 相关性评分: {card.relevance_score}")
            lines.append(f"- 精确算法标签: {', '.join(card.exact_method_tags) if card.exact_method_tags else '未识别'}")
            lines.append(f"- 问题标签: {', '.join(card.problem_tags) if card.problem_tags else '未识别'}")
            lines.append(f"- 研究提示: {card.one_line_takeaway}")
            lines.append(f"- 链接: {card.url if card.url else '未提供'}")
    else:
        lines.append("")
        lines.append("## 今日结果")
        lines.append("- 未取到可用论文。")

    if report.notes:
        lines.append("")
        lines.append("## 备注")
        for note in report.notes:
            lines.append(f"- {note}")

    return "\n".join(lines)


def _make_takeaway(method_tags: list[str], problem_tags: list[str]) -> str:
    if method_tags and problem_tags:
        return f"重点关注 {', '.join(problem_tags)} 场景，使用 {', '.join(method_tags)} 等精确求解。"
    if method_tags:
        return f"检测到 {', '.join(method_tags)} 相关精确算法，可优先阅读建模与约束定义。"
    if problem_tags:
        return f"主题聚焦 {', '.join(problem_tags)}，建议核对是否包含可证明最优的求解流程。"
    return "与列车调度有一定相关性，建议阅读全文确认模型与求解细节。"


def _normalize_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", title.lower())


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="列车调度文献日报生成器")
    parser.add_argument(
        "--config",
        default=str(Path("config") / "sources.yaml"),
        help="数据源配置文件路径",
    )
    parser.add_argument("--query", default=DEFAULT_QUERY, help="检索查询语句")
    parser.add_argument("--lookback-hours", type=int, default=24, help="回看窗口（小时）")
    parser.add_argument("--max-items", type=int, default=10, help="最多输出条数")
    return parser


def main() -> None:
    args = _build_cli().parse_args()
    report = run_daily_digest_from_file(
        config_path=args.config,
        query=args.query,
        lookback_hours=args.lookback_hours,
        max_items=args.max_items,
    )
    print(render_report_markdown(report))


if __name__ == "__main__":
    main()

