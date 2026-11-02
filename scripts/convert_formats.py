#!/usr/bin/env python
"""Convert the CSV input into Parquet (Snappy, Zstandard) and Delta Lake (Liquid Clustering).

    python scripts/convert_formats.py --sample              # laptop-sized run on data/sample/sample.csv
    python scripts/convert_formats.py                       # full file from data/raw
    python scripts/convert_formats.py --formats parquet_zstd delta

Outputs go to data/lake/<run>/<format>/ and a storage-footprint table is written to
results/simulated/storage_footprint[_sample].csv. The real schema and the inferred column
roles are saved to data/schema/<run>.json.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lakehouse.config import load_config, set_seeds  # noqa: E402
from lakehouse.data_io import (  # noqa: E402
    find_input_csv, infer_roles, read_csv, write_schema_json,
)
from lakehouse.storage_formats import (  # noqa: E402
    BYTES_PER_GB, data_size_bytes, get_spark, write_delta, write_parquet,
)

ALL_FORMATS = ("parquet_snappy", "parquet_zstd", "delta")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=None)
    ap.add_argument("--sample", action="store_true", help="use data/sample/sample.csv (local smoke run)")
    ap.add_argument("--formats", nargs="+", choices=ALL_FORMATS, default=list(ALL_FORMATS))
    ap.add_argument("--input", default=None, help="explicit CSV path (overrides config)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    set_seeds(cfg.seed)
    run = "sample" if args.sample else "full"
    src = Path(args.input) if args.input else find_input_csv(cfg, sample=args.sample)
    out_root = cfg.paths.lake_dir / run
    out_root.mkdir(parents=True, exist_ok=True)

    spark = get_spark(cfg, with_delta="delta" in args.formats)
    df = read_csv(spark, src)
    original_cols = spark.read.option("header", True).csv(str(src)).columns
    roles = infer_roles(df, cfg.columns, int(cfg.data["schema_probe_rows"]))
    write_schema_json(df, roles, original_cols, cfg.paths.schema_dir / f"{run}.json")
    print(f"[schema] roles = {roles.as_dict()}")

    csv_bytes = data_size_bytes(src)
    rows = [{"format": "csv", "codec": "none", "size_gb": csv_bytes / BYTES_PER_GB,
             "compression_ratio": 1.0, "write_seconds": 0.0}]

    for name in args.formats:
        if name.startswith("parquet"):
            res = write_parquet(df, out_root / name, cfg.formats[name]["codec"])
        else:
            res = write_delta(df, out_root / "delta", cfg, roles, table_name=f"lakehouse.telemetry_{run}")
        ratio = csv_bytes / (res.size_gb * BYTES_PER_GB) if res.size_gb else float("nan")
        print(f"[write] {res.fmt:<14} {res.codec:<7} {res.size_gb:.4f} GB  ratio {ratio:.2f}x  {res.seconds:.1f}s")
        rows.append({"format": res.fmt, "codec": res.codec, "size_gb": res.size_gb,
                     "compression_ratio": ratio, "write_seconds": res.seconds})

    results = cfg.paths.results_dir / "simulated"
    results.mkdir(parents=True, exist_ok=True)
    out_csv = results / ("storage_footprint_sample.csv" if args.sample else "storage_footprint.csv")
    with open(out_csv, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"[done] {out_csv}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
