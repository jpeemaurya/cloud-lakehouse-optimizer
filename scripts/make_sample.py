#!/usr/bin/env python
"""Create data/sample/sample.csv (<= data.sample_max_mb) from the downloaded CSV.

Rows are drawn by seeded Bernoulli sampling per chunk, so the sample is spread across the
whole file (not just its head) and is identical on every run.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lakehouse.config import load_config, set_seeds  # noqa: E402
from lakehouse.data_io import find_input_csv  # noqa: E402


def make_sample(src: Path, dst: Path, max_bytes: int, seed: int, chunksize: int = 100_000) -> int:
    frac = min(1.0, 0.9 * max_bytes / max(src.stat().st_size, 1))
    dst.parent.mkdir(parents=True, exist_ok=True)
    kept = []
    for i, chunk in enumerate(pd.read_csv(src, chunksize=chunksize, low_memory=False)):
        kept.append(chunk.sample(frac=frac, random_state=seed + i))
    df = pd.concat(kept, ignore_index=True)
    df.to_csv(dst, index=False)
    # Guard: trim rows until under the byte budget.
    while dst.stat().st_size > max_bytes and len(df) > 1:
        df = df.iloc[: int(len(df) * 0.9)]
        df.to_csv(dst, index=False)
    return len(df)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default=None)
    ap.add_argument("--input", default=None, help="CSV to sample (default: resolved from config)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    set_seeds(cfg.seed)
    src = Path(args.input) if args.input else find_input_csv(cfg)
    dst = cfg.paths.sample_dir / "sample.csv"
    n = make_sample(src, dst, int(cfg.data["sample_max_mb"] * 1024 * 1024), cfg.seed)
    print(f"[sample] {n} rows from {src.name} -> {dst} ({dst.stat().st_size / 1024:.0f} KiB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
