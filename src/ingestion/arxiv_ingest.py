"""arXiv ingestion -- bronze layer.

Fetches paper metadata from the official arXiv API and stores it as JSON Lines.

Subcommands
-----------
count   How many papers match the corpus query, plus a small sample (writes nothing).
fetch   Fetch the N most recent papers matching the query (the "rolling" corpus).
seed    Fetch the hand-picked foundational papers listed in config/seed_papers.json.

Storage layout
--------------
data/bronze/arxiv/runs/<run_id>.jsonl      raw records of a single run (immutable, for auditing)
data/bronze/arxiv/runs/<run_id>.meta.json  run parameters and statistics
data/bronze/arxiv/papers.jsonl             one record per paper, upserted by arXiv id

Every run is idempotent: re-running it never duplicates papers, it only inserts new ones
or replaces a paper when arXiv publishes a newer version.

Usage (from the project root)
-----------------------------
python -m src.ingestion.arxiv_ingest count --sample 10
python -m src.ingestion.arxiv_ingest fetch --max-results 500
python -m src.ingestion.arxiv_ingest fetch --incremental          # daily update
python -m src.ingestion.arxiv_ingest seed
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable, Iterator

import arxiv
import requests

log = logging.getLogger("arxiv_ingest")

# --------------------------------------------------------------------------- config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "bronze" / "arxiv"
DEFAULT_SEED_FILE = PROJECT_ROOT / "config" / "seed_papers.json"

API_URL = "https://export.arxiv.org/api/query"
OPENSEARCH_NS = "{http://a9.com/-/spec/opensearch/1.1/}"
USER_AGENT = "RAG-IA/0.1 (portfolio project; arXiv API metadata ingestion)"

# Corpus definition: LLM-based agents, within cs.AI (primary or cross-listed).
DEFAULT_QUERY = (
    "cat:cs.AI"
    ' AND (abs:LLM OR abs:LLMs OR abs:"language model" OR abs:"language models")'
    " AND (abs:agent OR abs:agents OR abs:agentic)"
)
DEFAULT_MAX_RESULTS = 500

# arXiv asks for at most one request every 3 seconds.
PAGE_SIZE = 100
DELAY_SECONDS = 3.0
NUM_RETRIES = 5

# --incremental stops after this many consecutive already-known papers.
KNOWN_STREAK_LIMIT = 20

_VERSION_RE = re.compile(r"v(\d+)$")


# --------------------------------------------------------------------------- helpers

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_query(base: str, since: date | None = None, until: date | None = None) -> str:
    """Add an optional submittedDate range to the base query."""
    if since is None and until is None:
        return base
    start = (since or date(1991, 1, 1)).strftime("%Y%m%d") + "0000"
    end = (until or utc_now().date()).strftime("%Y%m%d") + "2359"
    return f"({base}) AND submittedDate:[{start} TO {end}]"


def split_version(short_id: str) -> tuple[str, int | None]:
    """'2410.01234v2' -> ('2410.01234', 2); 'cs/0112017' -> ('cs/0112017', None)."""
    match = _VERSION_RE.search(short_id)
    if not match:
        return short_id, None
    return short_id[: match.start()], int(match.group(1))


def clean_text(text: str | None) -> str:
    """Collapse the hard line breaks and repeated spaces arXiv puts in titles/abstracts."""
    return " ".join((text or "").split())


def to_record(result: arxiv.Result, source: str, run_id: str) -> dict:
    """Flatten an arxiv.Result into a JSON-serialisable record."""
    arxiv_id, version = split_version(result.get_short_id())
    return {
        "arxiv_id": arxiv_id,
        "version": version,
        "title": clean_text(result.title),
        "authors": [a.name for a in result.authors],
        "abstract": clean_text(result.summary),
        "published": result.published.isoformat(),
        "updated": result.updated.isoformat(),
        "primary_category": result.primary_category,
        "categories": list(result.categories),
        "comment": result.comment,
        "journal_ref": result.journal_ref,
        "doi": result.doi,
        "abs_url": result.entry_id,
        "pdf_url": result.pdf_url,
        "sources": [source],
        "run_id": run_id,
        "ingested_at": utc_now().isoformat(timespec="seconds"),
    }


def make_client() -> arxiv.Client:
    return arxiv.Client(page_size=PAGE_SIZE, delay_seconds=DELAY_SECONDS, num_retries=NUM_RETRIES)


def iter_results(client: arxiv.Client, search: arxiv.Search) -> Iterator[arxiv.Result]:
    """Yield results, stopping gracefully (and keeping what we have) if the API fails mid-way."""
    try:
        yield from client.results(search)
    except (arxiv.ArxivError, requests.RequestException) as exc:
        log.warning("arXiv API stopped early (%s). Keeping the results fetched so far.", exc)


# --------------------------------------------------------------------------- storage

class PaperStore:
    """Deduplicated view of all ingested papers, keyed by arXiv id (without version)."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.path = data_dir / "papers.jsonl"
        self.papers: dict[str, dict] = {}
        if self.path.exists():
            with self.path.open(encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        rec = json.loads(line)
                        self.papers[rec["arxiv_id"]] = rec
        log.info("Store loaded: %d papers in %s", len(self.papers), self.path)

    def __contains__(self, arxiv_id: str) -> bool:
        return arxiv_id in self.papers

    def is_known(self, record: dict) -> bool:
        """True if we already hold this paper at the same or a newer version."""
        old = self.papers.get(record["arxiv_id"])
        return old is not None and (record["version"] or 0) <= (old.get("version") or 0)

    def upsert(self, records: Iterable[dict]) -> dict[str, int]:
        stats = {"inserted": 0, "updated": 0, "unchanged": 0}
        for rec in records:
            old = self.papers.get(rec["arxiv_id"])
            if old is None:
                rec["first_ingested_at"] = rec["ingested_at"]
                self.papers[rec["arxiv_id"]] = rec
                stats["inserted"] += 1
                continue

            sources = sorted(set(old.get("sources", [])) | set(rec["sources"]))
            if (rec["version"] or 0) > (old.get("version") or 0):
                rec["first_ingested_at"] = old.get("first_ingested_at", old["ingested_at"])
                rec["sources"] = sources
                self.papers[rec["arxiv_id"]] = rec
                stats["updated"] += 1
            elif sources != old.get("sources"):
                old["sources"] = sources  # e.g. a seed paper that also appears in the recent feed
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
        return stats

    def save(self) -> None:
        """Atomic write: a crash mid-save never leaves a half-written papers.jsonl."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".jsonl.tmp")
        ordered = sorted(self.papers.values(), key=lambda r: r["published"], reverse=True)
        with tmp.open("w", encoding="utf-8", newline="\n") as fh:
            for rec in ordered:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        os.replace(tmp, self.path)
        log.info("Store saved: %d papers -> %s", len(self.papers), self.path)


def write_run(data_dir: Path, run_id: str, records: list[dict], meta: dict) -> None:
    runs_dir = data_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    with (runs_dir / f"{run_id}.jsonl").open("w", encoding="utf-8", newline="\n") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    with (runs_dir / f"{run_id}.meta.json").open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)
    log.info("Run %s: %d raw records written to %s", run_id, len(records), runs_dir)


def new_run_id(command: str) -> str:
    return f"{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{command}"


# --------------------------------------------------------------------------- commands

def count_matches(query: str) -> int:
    """Ask the API for opensearch:totalResults, downloading at most one entry.

    Asks for 1 entry, not 0: the arXiv API answers max_results=0 with HTTP 500 (checked
    2026-10-06). It also retries transient 5xx errors with a growing pause, like the arxiv
    client does for 'fetch'.
    """
    params = {"search_query": query, "start": 0, "max_results": 1}
    for attempt in range(1, NUM_RETRIES + 1):
        try:
            resp = requests.get(API_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
            resp.raise_for_status()
            break
        except requests.RequestException as exc:
            status = getattr(exc.response, "status_code", None)
            retryable = status is None or status == 429 or status >= 500
            if not retryable or attempt == NUM_RETRIES:
                raise
            wait = DELAY_SECONDS * attempt
            log.warning("arXiv API error (%s). Retry %d/%d in %.0f s",
                        status or exc.__class__.__name__, attempt, NUM_RETRIES - 1, wait)
            time.sleep(wait)

    node = ET.fromstring(resp.content).find(f"{OPENSEARCH_NS}totalResults")
    if node is None or not (node.text or "").strip():
        raise RuntimeError("arXiv response has no opensearch:totalResults field")
    return int(node.text)


def cmd_count(args: argparse.Namespace) -> int:
    query = build_query(args.query, args.since, args.until)
    total = count_matches(query)
    print(f"Query : {query}")
    print(f"Total : {total} matching papers")
    if total > DEFAULT_MAX_RESULTS:
        print(f"        -> 'fetch' will keep the {DEFAULT_MAX_RESULTS} most recent by default.")

    if args.sample > 0:
        search = arxiv.Search(
            query=query,
            max_results=args.sample,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )
        print(f"\nMost recent {args.sample}:")
        for r in iter_results(make_client(), search):
            print(f"  {r.published:%Y-%m-%d}  [{r.primary_category:<8}]  {clean_text(r.title)}")
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    run_id = new_run_id("fetch")
    started = utc_now()
    query = build_query(args.query, args.since, args.until)
    store = PaperStore(args.data_dir)

    search = arxiv.Search(
        query=query,
        max_results=args.max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending,
    )
    log.info("Fetching up to %d papers for: %s", args.max_results, query)

    records: list[dict] = []
    known_streak = 0
    stopped_early = False
    for result in iter_results(make_client(), search):
        rec = to_record(result, source="recent", run_id=run_id)
        records.append(rec)
        if len(records) % 100 == 0:
            log.info("  ... %d fetched", len(records))
        if args.incremental:
            known_streak = known_streak + 1 if store.is_known(rec) else 0
            if known_streak >= KNOWN_STREAK_LIMIT:
                log.info("Incremental mode: %d known papers in a row, stopping.", known_streak)
                stopped_early = True
                break

    stats = store.upsert([dict(r) for r in records])
    store.save()
    write_run(args.data_dir, run_id, records, {
        "run_id": run_id,
        "command": "fetch",
        "query": query,
        "max_results": args.max_results,
        "incremental": args.incremental,
        "stopped_early": stopped_early,
        "fetched": len(records),
        **stats,
        "store_size": len(store.papers),
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": utc_now().isoformat(timespec="seconds"),
    })
    log.info("Done: fetched=%d inserted=%d updated=%d unchanged=%d | store=%d",
             len(records), stats["inserted"], stats["updated"], stats["unchanged"], len(store.papers))
    return 0


def _normalise_title(title: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", clean_text(title).lower())


def titles_match(expected: str, actual: str) -> bool:
    """Tolerant check that an id points at the paper we meant (titles change across versions)."""
    e, a = _normalise_title(expected), _normalise_title(actual)
    if SequenceMatcher(None, e, a).ratio() >= 0.75:
        return True
    # Most agent papers are named "<Name>: <subtitle>" -- same name is good enough.
    e_head, a_head = expected.split(":")[0], actual.split(":")[0]
    return ":" in expected and _normalise_title(e_head) == _normalise_title(a_head)


def cmd_seed(args: argparse.Namespace) -> int:
    run_id = new_run_id("seed")
    started = utc_now()
    seeds = json.loads(args.seed_file.read_text(encoding="utf-8"))["papers"]
    expected = {s["arxiv_id"]: s["title"] for s in seeds}
    log.info("Fetching %d seed papers from %s", len(expected), args.seed_file)

    search = arxiv.Search(id_list=list(expected), max_results=len(expected))
    records, rejected = [], []
    for result in iter_results(make_client(), search):
        rec = to_record(result, source="seed", run_id=run_id)
        want = expected.get(rec["arxiv_id"])
        if want is None:
            log.warning("Unexpected id returned by the API: %s", rec["arxiv_id"])
            continue
        if not titles_match(want, rec["title"]):
            log.error("Title mismatch for %s -- expected %r, got %r. Skipped: check the id.",
                      rec["arxiv_id"], want, rec["title"])
            rejected.append(rec["arxiv_id"])
            continue
        records.append(rec)

    missing = sorted(set(expected) - {r["arxiv_id"] for r in records} - set(rejected))
    for arxiv_id in missing:
        log.error("Seed paper not returned by the API: %s (%s)", arxiv_id, expected[arxiv_id])

    store = PaperStore(args.data_dir)
    stats = store.upsert([dict(r) for r in records])
    store.save()
    write_run(args.data_dir, run_id, records, {
        "run_id": run_id,
        "command": "seed",
        "seed_file": str(args.seed_file),
        "requested": len(expected),
        "fetched": len(records),
        "rejected_title_mismatch": rejected,
        "missing": missing,
        **stats,
        "store_size": len(store.papers),
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": utc_now().isoformat(timespec="seconds"),
    })
    log.info("Done: %d/%d seed papers stored (inserted=%d updated=%d unchanged=%d)",
             len(records), len(expected), stats["inserted"], stats["updated"], stats["unchanged"])
    return 1 if (missing or rejected) else 0


# --------------------------------------------------------------------------- CLI

def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="arxiv_ingest",
        description="Ingest arXiv paper metadata into the bronze layer.",
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR,
                        help=f"bronze output directory (default: {DEFAULT_DATA_DIR.relative_to(PROJECT_ROOT)})")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    sub = parser.add_subparsers(dest="command", required=True)

    def add_query_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--query", default=DEFAULT_QUERY, help="arXiv search_query (default: LLM agents in cs.AI)")
        p.add_argument("--since", type=_date, help="only papers submitted on/after YYYY-MM-DD")
        p.add_argument("--until", type=_date, help="only papers submitted on/before YYYY-MM-DD")

    p_count = sub.add_parser("count", help="count matching papers and show a sample (writes nothing)")
    add_query_args(p_count)
    p_count.add_argument("--sample", type=int, default=5, help="how many recent titles to show (default: 5)")
    p_count.set_defaults(func=cmd_count)

    p_fetch = sub.add_parser("fetch", help="fetch the most recent matching papers")
    add_query_args(p_fetch)
    p_fetch.add_argument("--max-results", type=int, default=DEFAULT_MAX_RESULTS,
                         help=f"maximum papers to fetch (default: {DEFAULT_MAX_RESULTS})")
    p_fetch.add_argument("--incremental", action="store_true",
                         help=f"stop after {KNOWN_STREAK_LIMIT} consecutive already-stored papers")
    p_fetch.set_defaults(func=cmd_fetch)

    p_seed = sub.add_parser("seed", help="fetch the hand-picked foundational papers")
    p_seed.add_argument("--seed-file", type=Path, default=DEFAULT_SEED_FILE)
    p_seed.set_defaults(func=cmd_seed)
    return parser


def main(argv: list[str] | None = None) -> int:
    # Windows consoles (cp1252) choke on non-ASCII paper titles; never crash on printing.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")
    try:
        return args.func(args)
    except requests.RequestException as exc:
        log.error("Could not reach the arXiv API: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
