#!/usr/bin/env python3
"""Copy a verified frozen RS Leader 30m bundle to a new replay book ID.

The source is never modified. Candle JSON bytes are copied verbatim; only the
top-level audit book_id and its fingerprint change.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_martin_intraday_execution import digest


def rebind_bundle(source: str | Path, destination: str | Path, book_id: str) -> None:
    source, destination = Path(source), Path(destination)
    if not book_id.startswith("rs_leader_30m_pivot_replay_"):
        raise ValueError("book_id must start with rs_leader_30m_pivot_replay_")
    if not source.is_dir() or destination.exists():
        raise ValueError("source must exist and destination must not exist")
    manifest_path = source / "audit.json"
    manifest = json.loads(manifest_path.read_text())
    fingerprint = manifest.pop("fingerprint", None)
    if not fingerprint or digest(manifest) != fingerprint:
        raise ValueError("frozen_30m_manifest_fingerprint_mismatch")
    files = []
    seen = set()
    for record in manifest.get("records", []):
        filename = record.get("candle_file")
        if not filename:
            continue
        if Path(filename).name != filename or not filename.endswith(".json") or filename in seen:
            raise ValueError("frozen_30m_bundle_path_invalid")
        seen.add(filename)
        path = source / filename
        data = json.loads(path.read_text())
        if digest(data) != record.get("candle_fingerprint"):
            raise ValueError(f"frozen_30m_bundle_fingerprint_mismatch:{record.get('ticker', '')}")
        files.append((filename, path.read_bytes()))
    output = {**manifest, "book_id": book_id}
    output["fingerprint"] = digest(output)
    destination.mkdir(parents=True)
    try:
        for filename, contents in files:
            (destination / filename).write_bytes(contents)
        (destination / "audit.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    except Exception:
        shutil.rmtree(destination)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source")
    parser.add_argument("destination")
    parser.add_argument("--book-id", required=True)
    args = parser.parse_args(argv)
    try:
        rebind_bundle(args.source, args.destination, args.book_id)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps({"ready": True, "source": str(Path(args.source)),
                      "destination": str(Path(args.destination)), "book_id": args.book_id}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
