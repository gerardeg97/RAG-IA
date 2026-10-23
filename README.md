# RAG-IA

RAG pipeline for AI research papers — from arXiv ingestion to evaluated retrieval, with orchestration via Airflow/Dagster and FastAPI serving.

The goal is not another notebook that calls an LLM, but a data-engineering view of RAG: a real ingestion pipeline, incremental updates, quantitative evaluation and deployment.

> **Status:** Phase 1 in progress — arXiv metadata and PDF ingestion are implemented (`src/ingestion/arxiv_ingest.py`). The bronze layer holds **520 papers** (500 recent, submitted 26 Sep – 5 Oct 2026, plus 20 seed papers) and their **520 PDFs** (1.6 GB). Next: text extraction (silver).

---

## Corpus

### Scope: LLM-based agents in cs.AI

The corpus is arXiv papers listed in **cs.AI** (primary or cross-listed) whose abstract is about **LLM-based agents**: tool use, planning, memory, self-reflection and multi-agent systems.

```text
cat:cs.AI
  AND (abs:LLM OR abs:LLMs OR abs:"language model" OR abs:"language models")
  AND (abs:agent OR abs:agents OR abs:agentic)
```

### Why a topic filter instead of a date window

cs.AI is too big to scope by date alone. It received **45,133 submissions in 2025** (~3,800 a month), up from 14,806 in 2022 ([source](https://presenc.ai/research/arxiv-ai-paper-volume-2020-2025)). A "last 12 months of cs.AI" corpus would hold tens of thousands of papers on unrelated subjects. Any 500-paper slice of it would be arbitrary and incoherent, which hurts retrieval quality and makes evaluation noisy.

A single, coherent topic keeps retrieval meaningful and lets the evaluation questions be written and checked by hand.

### Why the 500 most recent papers (a rolling corpus)

The corpus is the **N most recent matching papers** (N = 500 by default), not a sample from a fixed time window:

- **Retrieval shows its value.** Papers from the last few weeks are not in any LLM's training data, so the difference between answering *with* and *without* retrieval is large and measurable.
- **Incremental ingestion is a real requirement.** After the initial load, the pipeline adds new papers every day (`fetch --incremental`). That is what justifies orchestration, idempotency and upserts, rather than a one-off download.
- **No sampling decisions.** Sorting by submission date and taking the top N is deterministic and reproducible.

N and the date range are CLI parameters, so the strategy can change without touching code.

### Foundational seed papers

Recent agent papers build on a small set of earlier works (ReAct, Toolformer, Reflexion, Generative Agents…) and cite them constantly, but rarely explain them. Without those papers in the corpus:

1. Natural questions like *"What is ReAct?"* or *"How does Reflexion differ from Self-Refine?"* only retrieve second-hand mentions from related-work sections.
2. The LLM fills the gaps from its own memory, so answers stop being grounded in the corpus, and faithfulness metrics become harder to interpret.
3. The corpus has variations on ideas but not the ideas themselves.

So **20 foundational papers are added by hand** (`config/seed_papers.json`). They are tagged `seed` so that evaluation can be broken down by question type:

| Question type | Needs | Expected RAG gain |
|---|---|---|
| Conceptual | seed papers | lower — LLMs already know these works |
| Current | recent papers | higher — unseen by the LLM |

### Known limitations

- The keyword filter is lexical: it can miss relevant papers that avoid the words "agent" or "LLM", and it can include papers that only mention them in passing.
- 500 recent papers cover only a few weeks of publications, so the corpus reflects current work, not the whole history of the field.
- Many foundational works are listed under cs.CL or cs.LG, not cs.AI. This is why they are curated by hand rather than found by the query.

---

## Architecture

```text
Data sources → Ingestion/ETL → Chunking → Embeddings → Vector DB
                                                          ↓
User → API (FastAPI) → Retrieval → Prompt + context → LLM → Answer
                                                          ↓
                                                Evaluation / logging
```

Data follows a medallion layout: **bronze** (raw, as fetched) → **silver** (clean text) → **gold** (chunks + embeddings).

---

## Getting started

Requires Python 3.10+.

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows  (Linux/macOS: source .venv/bin/activate)
pip install -r requirements.txt
```

### Ingestion (arXiv metadata and PDFs)

Run from the project root:

```bash
# 1. Explore: how many papers match, and what do the most recent look like? (writes nothing)
python -m src.ingestion.arxiv_ingest count --sample 10

# 2. Initial load: the 500 most recent matching papers
python -m src.ingestion.arxiv_ingest fetch

# 3. Foundational papers
python -m src.ingestion.arxiv_ingest seed

# 4. PDFs of every stored paper (try a few first with --limit 3)
python -m src.ingestion.arxiv_ingest pdfs

# 5. Daily update: stops after 20 consecutive papers already stored, then fetch their PDFs
python -m src.ingestion.arxiv_ingest fetch --incremental
python -m src.ingestion.arxiv_ingest pdfs
```

Useful options: `--max-results N`, `--since YYYY-MM-DD`, `--until YYYY-MM-DD`, `--query "<arXiv query>"`, `--log-level DEBUG`.

The client respects arXiv's API guidelines: pages of 100 results, at least 3 seconds between requests, and retries on failure. If the API fails mid-run, the results fetched so far are kept.

`pdfs` downloads one PDF per paper, at least 3 seconds apart (about 30 minutes for the full corpus):

- **Skips** papers whose PDF for the stored version is already on disk, so it can be stopped and resumed at any time.
- **Atomic writes:** each file is streamed to `<name>.pdf.tmp` and only renamed once complete and checked to start with `%PDF-`, so an interrupted run never leaves a truncated PDF.
- **New versions:** when a paper gets a new version, the new PDF is downloaded first and the old one is deleted only afterwards.
- **Failures** never stop the run. Transient errors (timeouts, dropped connections, HTTP 429/5xx) are tried up to 3 times; permanent ones (other 4xx, non-PDF responses) are not. Both are listed in the run's `.meta.json` (`transitory_fail`, `permanent_fail`) and retried on the next run.

### Data layout

```text
data/bronze/arxiv/
├── papers.jsonl                 # one record per paper, upserted by arXiv id
├── pdfs/
│   └── <arxiv_id>v<version>.pdf # latest stored version of each paper
└── runs/
    ├── <run_id>.jsonl           # raw records of one run (immutable, for auditing)
    └── <run_id>.meta.json       # parameters and counts (inserted/updated/unchanged, downloaded/failed)
```

Runs are **idempotent**: running the same command twice never duplicates papers or PDFs. A paper is only replaced when arXiv publishes a newer version, and its `first_ingested_at` is preserved. `data/` is git-ignored.

Main fields of each record:

| Field | Description |
|---|---|
| `arxiv_id`, `version` | id without version (dedup key) and version number |
| `title`, `abstract`, `authors` | text with arXiv's hard line breaks removed |
| `published`, `updated` | first submission and last update (ISO 8601, UTC) |
| `primary_category`, `categories` | arXiv categories |
| `pdf_url`, `abs_url` | links used in the next phase and for citing sources |
| `sources` | `recent`, `seed`, or both |
| `run_id`, `ingested_at`, `first_ingested_at` | lineage |

---

## Roadmap

| Phase | Content | Status |
|---|---|---|
| 0 | Domain and corpus definition | ✅ done |
| 1 | Ingestion: arXiv metadata (bronze) → PDF download → text extraction (silver) | 🟡 metadata and PDFs done |
| 2 | Chunking and embeddings; chunk-size experiment (256 / 512 / 1024 tokens) | ⬜ |
| 3 | Orchestration as a DAG (`fetch → clean → chunk → embed → load`), incremental updates | ⬜ |
| 4 | Retrieval + generation behind a FastAPI `/query` endpoint, with source citations | ⬜ |
| 5 | Evaluation with RAGAS: RAG vs no-RAG, conceptual vs current questions | ⬜ |
| 6 | Docker, deployment and a minimal Streamlit UI | ⬜ |

## Project structure

```text
RAG-IA/
├── config/
│   └── seed_papers.json         # hand-picked foundational papers
├── src/
│   └── ingestion/
│       └── arxiv_ingest.py      # arXiv API → bronze layer
├── data/                        # generated, git-ignored
└── requirements.txt
```

## License

GPL-3.0 — see [LICENSE](LICENSE).
