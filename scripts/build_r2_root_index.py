#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from html import escape
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update an R2 root manifest and render a bucket landing page."
    )
    parser.add_argument("--manifest-in", required=True, help="Existing manifest path.")
    parser.add_argument("--manifest-out", required=True, help="Updated manifest path.")
    parser.add_argument("--html-out", required=True, help="Output HTML path.")
    parser.add_argument("--pipeline-id", required=True, help="Stable pipeline id, e.g. rs-screen.")
    parser.add_argument("--pipeline-name", required=True, help="Human-readable pipeline name.")
    parser.add_argument("--date-label", required=True, help="Run date label.")
    parser.add_argument("--summary-file", default="", help="Optional run summary JSON to derive metrics from.")
    parser.add_argument("--total-tickers", type=int, default=None)
    parser.add_argument("--passed-tickers", type=int, default=None)
    parser.add_argument("--failure-count", type=int, default=None)
    parser.add_argument("--rendered-index-url", required=True)
    parser.add_argument("--watchlist-url", required=True)
    parser.add_argument("--raw-results-url", required=True)
    parser.add_argument("--run-url", default="")
    parser.add_argument("--source-label", default="")
    return parser.parse_args()


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"updated_at": None, "entries": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"updated_at": None, "entries": []}
    if not isinstance(data, dict):
        return {"updated_at": None, "entries": []}
    entries = data.get("entries")
    if not isinstance(entries, list):
        entries = []
    return {"updated_at": data.get("updated_at"), "entries": entries}


def derive_metrics(args: argparse.Namespace) -> tuple[int, int, int, str]:
    total_tickers = args.total_tickers
    passed_tickers = args.passed_tickers
    failure_count = args.failure_count
    source_label = args.source_label

    if args.summary_file:
        summary_path = Path(args.summary_file)
        if summary_path.exists():
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if total_tickers is None:
                total_tickers = int(summary.get("total_tickers", 0) or 0)
            if passed_tickers is None:
                passed_tickers = int(summary.get("passed_tickers", 0) or 0)
            if failure_count is None:
                failures = summary.get("failed_tickers", [])
                if isinstance(failures, list):
                    failure_count = len(failures)
                else:
                    failure_count = int(failures or 0)
            if not source_label:
                source = str(summary.get("source", "")).strip()
                reference_date = str(summary.get("reference_date", "")).strip()
                if source and reference_date and reference_date.lower() != "none":
                    source_label = f"{source} ({reference_date})"
                elif source:
                    source_label = source

    return int(total_tickers or 0), int(passed_tickers or 0), int(failure_count or 0), source_label


def upsert_entry(entries: list[dict[str, Any]], entry: dict[str, Any]) -> list[dict[str, Any]]:
    entry_id = entry["id"]
    kept = [item for item in entries if isinstance(item, dict) and item.get("id") != entry_id]
    kept.append(entry)
    kept.sort(key=lambda item: (str(item.get("date_label", "")), str(item.get("pipeline_id", ""))), reverse=True)
    return kept


def render_root_index(manifest: dict[str, Any]) -> str:
    entries = [
        item
        for item in manifest.get("entries", [])
        if isinstance(item, dict)
    ]
    entries.sort(key=lambda item: (str(item.get("date_label", "")), str(item.get("pipeline_id", ""))), reverse=True)

    pipeline_names: dict[str, str] = {}
    latest_by_pipeline: dict[str, dict[str, Any]] = {}
    for item in entries:
        pipeline_id = str(item.get("pipeline_id", "")).strip()
        pipeline_name = str(item.get("pipeline_name", pipeline_id)).strip() or pipeline_id
        if pipeline_id:
            pipeline_names[pipeline_id] = pipeline_name
            latest_by_pipeline.setdefault(pipeline_id, item)

    cards_html: list[str] = []
    for item in entries:
        pipeline_id = str(item.get("pipeline_id", "")).strip()
        pipeline_name = str(item.get("pipeline_name", pipeline_id)).strip() or pipeline_id
        date_label = str(item.get("date_label", "")).strip()
        source_label = str(item.get("source_label", "")).strip()
        rendered_index_url = str(item.get("rendered_index_url", "")).strip()
        watchlist_url = str(item.get("watchlist_url", "")).strip()
        raw_results_url = str(item.get("raw_results_url", "")).strip()
        run_url = str(item.get("run_url", "")).strip()
        total_tickers = int(item.get("total_tickers", 0) or 0)
        passed_tickers = int(item.get("passed_tickers", 0) or 0)
        failure_count = int(item.get("failure_count", 0) or 0)
        search_blob = " ".join(
            value for value in [pipeline_id, pipeline_name, date_label, source_label] if value
        ).lower()

        meta_parts = [
            f"<span><strong>{passed_tickers}</strong> hits</span>",
            f"<span><strong>{total_tickers}</strong> screened</span>",
            f"<span><strong>{failure_count}</strong> failures</span>",
        ]
        if source_label:
            meta_parts.append(f"<span>{escape(source_label)}</span>")

        links = [
            f'<a href="{escape(rendered_index_url)}">Rendered Charts</a>',
            f'<a href="{escape(watchlist_url)}">Watchlist JSON</a>',
            f'<a href="{escape(raw_results_url)}">Raw Results</a>',
        ]
        if run_url:
            links.append(f'<a href="{escape(run_url)}">GitHub Run</a>')

        cards_html.append(
            f'''
            <article class="run-card" data-pipeline="{escape(pipeline_id)}" data-date="{escape(date_label)}"
              data-hits="{passed_tickers}" data-search="{escape(search_blob)}">
              <div class="card-header">
                <div>
                  <p class="eyebrow">{escape(pipeline_name)}</p>
                  <h3>{escape(date_label)}</h3>
                </div>
                <a class="primary-link" href="{escape(rendered_index_url)}">Open</a>
              </div>
              <p class="meta-row">{"".join(meta_parts)}</p>
              <p class="link-row">{"".join(links)}</p>
            </article>
            '''
        )

    latest_links: list[str] = []
    for pipeline_id, item in sorted(latest_by_pipeline.items(), key=lambda pair: pipeline_names.get(pair[0], pair[0])):
        latest_links.append(
            f'<a href="{escape(str(item.get("rendered_index_url", "")))}">{escape(pipeline_names.get(pipeline_id, pipeline_id))}</a>'
        )

    pipeline_options = ['<option value="all">All workflows</option>']
    for pipeline_id, pipeline_name in sorted(pipeline_names.items(), key=lambda pair: pair[1]):
        pipeline_options.append(
            f'<option value="{escape(pipeline_id)}">{escape(pipeline_name)}</option>'
        )

    updated_at = str(manifest.get("updated_at", "")).strip() or "unknown"
    run_count = len(entries)
    latest_count = len(latest_by_pipeline)
    cards_markup = "\n".join(cards_html) if cards_html else '<p class="empty">No published runs yet.</p>'
    latest_markup = " ".join(latest_links) if latest_links else "<span>No runs yet.</span>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Ticker Screener Watchlists</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --paper: #fffdf8;
      --ink: #1f2a2e;
      --muted: #5f6b70;
      --accent: #0a7f78;
      --accent-2: #d98b2b;
      --border: #d8d0c0;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      background:
        radial-gradient(circle at top left, rgba(217,139,43,0.12), transparent 28%),
        linear-gradient(180deg, #fbf7ef 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    main {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .hero {{
      display: grid;
      gap: 18px;
      padding: 28px;
      border: 1px solid var(--border);
      background: rgba(255,253,248,0.92);
      border-radius: 24px;
      box-shadow: 0 18px 40px rgba(31,42,46,0.08);
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2rem, 4vw, 3.8rem);
      line-height: 0.95;
      letter-spacing: -0.03em;
    }}
    .hero p {{
      margin: 0;
      max-width: 760px;
      color: var(--muted);
      font-size: 1.05rem;
      line-height: 1.6;
    }}
    .latest-links, .filters, .stats, .grid, .meta-row, .link-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }}
    .latest-links a, .link-row a, .primary-link {{
      color: var(--accent);
      text-decoration: none;
      font-weight: 700;
    }}
    .stats {{
      margin-top: 8px;
    }}
    .stat {{
      padding: 10px 14px;
      border-radius: 999px;
      background: #f0ece3;
      border: 1px solid var(--border);
      font-size: 0.95rem;
    }}
    .controls {{
      margin-top: 24px;
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: rgba(255,255,255,0.72);
      backdrop-filter: blur(10px);
    }}
    .philosophy {{
      margin-top: 24px;
      padding: 22px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: rgba(255,253,248,0.86);
    }}
    .philosophy h2 {{
      margin: 0 0 12px;
      font-size: 1.4rem;
    }}
    .philosophy-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
    }}
    .philosophy-card {{
      padding: 16px;
      border: 1px solid var(--border);
      border-radius: 16px;
      background: rgba(255,255,255,0.6);
    }}
    .philosophy-card h3 {{
      margin: 0 0 8px;
      font-size: 1.05rem;
    }}
    .philosophy-card p {{
      margin: 0;
      color: var(--muted);
      font-size: 0.96rem;
      line-height: 1.55;
    }}
    .filters {{
      align-items: end;
    }}
    label {{
      display: grid;
      gap: 6px;
      font-size: 0.9rem;
      color: var(--muted);
      min-width: 180px;
      flex: 1 1 180px;
    }}
    input, select {{
      width: 100%;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: 12px;
      background: var(--paper);
      font: inherit;
      color: var(--ink);
    }}
    .count-label {{
      margin-top: 14px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .grid {{
      margin-top: 22px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }}
    .run-card {{
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: rgba(255,253,248,0.95);
      box-shadow: 0 14px 28px rgba(31,42,46,0.06);
    }}
    .card-header {{
      display: flex;
      align-items: start;
      justify-content: space-between;
      gap: 10px;
    }}
    .eyebrow {{
      margin: 0 0 8px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.72rem;
      color: var(--accent-2);
      font-weight: 700;
    }}
    .run-card h3 {{
      margin: 0;
      font-size: 1.45rem;
    }}
    .meta-row, .link-row {{
      margin-top: 14px;
      color: var(--muted);
      font-size: 0.94rem;
    }}
    .meta-row span {{
      padding: 7px 10px;
      border-radius: 999px;
      background: #f5f1e8;
      border: 1px solid var(--border);
    }}
    .empty {{
      color: var(--muted);
      font-style: italic;
    }}
    @media (max-width: 720px) {{
      main {{ padding: 20px 14px 40px; }}
      .hero {{ padding: 22px; }}
      .controls {{ padding: 14px; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div>
        <p class="eyebrow">Ticker Screener</p>
        <h1>Watchlists at a Glance</h1>
      </div>
      <p>Browse the latest published watchlists across workflows, then filter down by strategy, date, text, or minimum hit count. Each card links straight to the rendered charts plus the underlying JSON artifacts.</p>
      <div class="latest-links"><strong>Latest:</strong> {latest_markup}</div>
      <div class="stats">
        <span class="stat">{run_count} published runs</span>
        <span class="stat">{latest_count} workflow families</span>
        <span class="stat">Updated {escape(updated_at)}</span>
      </div>
    </section>

    <section class="philosophy">
      <h2>Screening Philosophy</h2>
      <div class="philosophy-grid">
        <article class="philosophy-card">
          <h3>RS New High Before Price</h3>
          <p>This looks for names where the relative-strength line is already breaking to a new high before price does. The idea is that hidden leadership often shows up in RS first, before the chart itself looks obvious to everyone else.</p>
        </article>
        <article class="philosophy-card">
          <h3>PEG Earnings Gap</h3>
          <p>This focuses on powerful upside earnings gaps with strong volume and strong closes. The goal is to find names where fresh information forced institutions to reprice the stock higher, creating a gap that may continue rather than fade.</p>
        </article>
        <article class="philosophy-card">
          <h3>Pre-Earnings Focus</h3>
          <p>This is a quality filter ahead of earnings. It favors stocks with healthy trend, tight action, good liquidity, and constructive relative strength, aiming to surface names that are acting well before the catalyst arrives.</p>
        </article>
      </div>
    </section>

    <section class="controls">
      <div class="filters">
        <label>
          Search
          <input id="searchInput" type="search" placeholder="Try rs, peg, 2026-05, manual...">
        </label>
        <label>
          Workflow
          <select id="pipelineFilter">
            {"".join(pipeline_options)}
          </select>
        </label>
        <label>
          Minimum Hits
          <input id="minHitsFilter" type="number" min="0" step="1" value="0">
        </label>
        <label>
          Sort
          <select id="sortFilter">
            <option value="date-desc">Newest first</option>
            <option value="date-asc">Oldest first</option>
            <option value="hits-desc">Most hits</option>
            <option value="hits-asc">Fewest hits</option>
          </select>
        </label>
      </div>
      <p class="count-label" id="countLabel">Showing {run_count} runs</p>
    </section>

    <section class="grid" id="cardGrid">
      {cards_markup}
    </section>
  </main>

  <script>
    const grid = document.getElementById("cardGrid");
    const cards = Array.from(grid.querySelectorAll(".run-card"));
    const searchInput = document.getElementById("searchInput");
    const pipelineFilter = document.getElementById("pipelineFilter");
    const minHitsFilter = document.getElementById("minHitsFilter");
    const sortFilter = document.getElementById("sortFilter");
    const countLabel = document.getElementById("countLabel");

    function compareCards(a, b, sortValue) {{
      const aDate = a.dataset.date || "";
      const bDate = b.dataset.date || "";
      const aHits = Number(a.dataset.hits || "0");
      const bHits = Number(b.dataset.hits || "0");
      if (sortValue === "date-asc") return aDate.localeCompare(bDate);
      if (sortValue === "hits-desc") return bHits - aHits || bDate.localeCompare(aDate);
      if (sortValue === "hits-asc") return aHits - bHits || aDate.localeCompare(bDate);
      return bDate.localeCompare(aDate);
    }}

    function applyFilters() {{
      const query = searchInput.value.trim().toLowerCase();
      const selectedPipeline = pipelineFilter.value;
      const minHits = Number(minHitsFilter.value || "0");
      const sortValue = sortFilter.value;

      const filtered = cards.filter((card) => {{
        const matchesPipeline = selectedPipeline === "all" || card.dataset.pipeline === selectedPipeline;
        const hits = Number(card.dataset.hits || "0");
        const matchesHits = hits >= minHits;
        const haystack = card.dataset.search || "";
        const matchesQuery = !query || haystack.includes(query);
        const visible = matchesPipeline && matchesHits && matchesQuery;
        card.style.display = visible ? "" : "none";
        return visible;
      }});

      filtered.sort((a, b) => compareCards(a, b, sortValue)).forEach((card) => grid.appendChild(card));
      countLabel.textContent = `Showing ${{filtered.length}} run${{filtered.length === 1 ? "" : "s"}}`;
    }}

    [searchInput, pipelineFilter, minHitsFilter, sortFilter].forEach((input) => {{
      input.addEventListener("input", applyFilters);
      input.addEventListener("change", applyFilters);
    }});
    applyFilters();
  </script>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    manifest_in = Path(args.manifest_in)
    manifest_out = Path(args.manifest_out)
    html_out = Path(args.html_out)
    manifest_out.parent.mkdir(parents=True, exist_ok=True)
    html_out.parent.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(manifest_in)
    total_tickers, passed_tickers, failure_count, source_label = derive_metrics(args)
    entry = {
        "id": f"{args.pipeline_id}:{args.date_label}",
        "pipeline_id": args.pipeline_id,
        "pipeline_name": args.pipeline_name,
        "date_label": args.date_label,
        "total_tickers": total_tickers,
        "passed_tickers": passed_tickers,
        "failure_count": failure_count,
        "rendered_index_url": args.rendered_index_url,
        "watchlist_url": args.watchlist_url,
        "raw_results_url": args.raw_results_url,
        "run_url": args.run_url,
        "source_label": source_label,
    }
    manifest["entries"] = upsert_entry(list(manifest.get("entries", [])), entry)
    manifest["updated_at"] = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

    manifest_out.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    html_out.write_text(render_root_index(manifest), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
