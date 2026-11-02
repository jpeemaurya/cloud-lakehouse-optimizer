#!/usr/bin/env python
"""Download the Kaggle sources cited in the paper and verify SHA-256 checksums.

Credentials are read by the Kaggle client from ~/.kaggle/kaggle.json (chmod 600) or from the
KAGGLE_USERNAME / KAGGLE_KEY environment variables. Nothing is hard-coded here.

Examples
--------
    python scripts/download_data.py                    # download + verify
    python scripts/download_data.py --write-checksums  # first run: record digests
    python scripts/download_data.py --strict           # fail if any file lacks a digest
"""
from __future__ import annotations

import argparse
import os
import stat
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lakehouse.config import load_config  # noqa: E402
from lakehouse.data_io import load_checksums, save_checksums, verify_files  # noqa: E402


def check_credentials() -> None:
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return
    if not kaggle_json.is_file():
        sys.exit(
            "Kaggle credentials not found.\n"
            "Create an API token at https://www.kaggle.com/settings -> 'Create New Token', save it as\n"
            "~/.kaggle/kaggle.json and run: chmod 600 ~/.kaggle/kaggle.json"
        )
    if kaggle_json.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        print("WARNING: ~/.kaggle/kaggle.json is group/world readable; run chmod 600 on it.", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=None)
    ap.add_argument("--only", default=None, help="download a single source by name")
    ap.add_argument("--write-checksums", action="store_true", help="record/refresh digests in data/checksums.sha256")
    ap.add_argument("--strict", action="store_true", help="exit non-zero if any file has no recorded digest")
    ap.add_argument("--skip-download", action="store_true", help="only verify files already on disk")
    args = ap.parse_args()

    cfg = load_config(args.config)
    sources = [s for s in cfg.sources if args.only in (None, s.name)]
    if not sources:
        sys.exit(f"No source named {args.only!r}")

    if not args.skip_download:
        check_credentials()
        from kaggle.api.kaggle_api_extended import KaggleApi  # imported late: it authenticates on use

        api = KaggleApi()
        api.authenticate()
        for s in sources:
            dest = cfg.paths.raw_dir / s.name
            dest.mkdir(parents=True, exist_ok=True)
            print(f"[download] {s.kind}: {s.slug} -> {dest}")
            if s.kind == "dataset":
                api.dataset_download_files(s.slug, path=str(dest), unzip=True, quiet=False)
            elif s.kind == "kernel":
                api.kernels_pull(s.slug, path=str(dest), metadata=True)
            else:
                sys.exit(f"Unknown source kind {s.kind!r}")

    known = load_checksums(cfg.paths.checksums)
    all_computed: dict[str, str] = dict(known)
    bad, unverified = [], []
    for s in sources:
        d = cfg.paths.raw_dir / s.name
        files = sorted(f for f in d.rglob("*") if f.is_file()) if d.exists() else []
        mism, unv, comp = verify_files(files, cfg.paths.raw_dir, known)
        bad += mism
        unverified += unv
        all_computed.update(comp)

    if args.write_checksums:
        save_checksums(cfg.paths.checksums, all_computed)
        print(f"[checksums] wrote {len(all_computed)} digest(s) to {cfg.paths.checksums}")
        return 0
    if bad:
        print("CHECKSUM MISMATCH:\n  " + "\n  ".join(bad), file=sys.stderr)
        return 2
    if unverified:
        print("No recorded digest for:\n  " + "\n  ".join(unverified), file=sys.stderr)
        print("Run with --write-checksums once and commit data/checksums.sha256.", file=sys.stderr)
        return 3 if args.strict else 0
    print("[checksums] all files verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
