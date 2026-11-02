"""Data access: locating the input CSV, schema inspection, column-role inference,
and SHA-256 checksum handling.

Nothing here assumes column names. Roles (time / key / category / measure) are
derived from the *actual* Spark schema and value statistics, or taken from
``config.yaml`` when the user overrides them.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .config import Config

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

NUMERIC_TYPES = {"tinyint", "smallint", "int", "bigint", "float", "double"}
TIME_TYPES = {"timestamp", "date", "timestamp_ntz"}


class SchemaInferenceError(RuntimeError):
    """Raised when a column role cannot be inferred; set it in config.yaml."""


# --------------------------------------------------------------------------- #
# Locating input
# --------------------------------------------------------------------------- #
def find_input_csv(cfg: Config, sample: bool = False) -> Path:
    """Return the CSV the benchmark should read.

    ``sample=True`` -> ``data/sample/sample.csv``. Otherwise ``data.input_file`` if set,
    else the largest ``*.csv`` below ``raw_dir/<primary_source>``.
    """
    if sample:
        p = cfg.paths.sample_dir / "sample.csv"
        if not p.is_file():
            raise FileNotFoundError(f"{p} not found. Run `make sample` first.")
        return p
    explicit = cfg.data.get("input_file")
    if explicit:
        p = (cfg.root / explicit).resolve()
        if not p.is_file():
            raise FileNotFoundError(p)
        return p
    base = cfg.paths.raw_dir / cfg.data["primary_source"]
    csvs = sorted(base.rglob("*.csv"), key=lambda f: f.stat().st_size, reverse=True)
    if not csvs:
        raise FileNotFoundError(
            f"No CSV found under {base}. Run `make data` (needs Kaggle credentials)."
        )
    return csvs[0]


def normalise_name(name: str) -> str:
    """snake_case a column name; Parquet/Delta reject spaces and ``,;{}()=``."""
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    s = s or "col"
    return f"c_{s}" if s[0].isdigit() else s


def read_csv(spark: "SparkSession", path: str | Path) -> "DataFrame":
    """Read a CSV with header + type inference and snake_case the column names."""
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    seen: dict[str, int] = {}
    new_names = []
    for c in df.columns:
        n = normalise_name(c)
        seen[n] = seen.get(n, 0) + 1
        new_names.append(n if seen[n] == 1 else f"{n}_{seen[n]}")
    return df.toDF(*new_names)


# --------------------------------------------------------------------------- #
# Column roles
# --------------------------------------------------------------------------- #
@dataclass
class ColumnRoles:
    key: str
    category: str
    measure: str
    time: str | None = None
    category2: str | None = None
    measure2: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)


def infer_roles(df: "DataFrame", overrides: dict[str, str | None] | None = None,
                probe_rows: int = 50_000) -> ColumnRoles:
    """Derive :class:`ColumnRoles` from the real schema and value statistics.

    Explicit entries in ``overrides`` (from ``config.yaml: columns``) win.
    Heuristics for the remainder:

    * time      -> first timestamp/date column, else a string column that parses as a
                   timestamp for >=95 % of non-null probe rows.
    * key       -> highest-cardinality string column (integer column if no string
                   column exists); a name containing ``id`` is preferred on ties of kind.
    * category  -> lowest-cardinality (>=2) remaining string column; category2 the next.
    * measure   -> highest-cardinality numeric column that is not the key; measure2 next.
    """
    from pyspark.sql import functions as F

    ov = {k: v for k, v in (overrides or {}).items() if v}
    for role, col in ov.items():
        if col not in df.columns:
            raise SchemaInferenceError(
                f"config.yaml columns.{role}='{col}' is not in the data. Columns: {df.columns}"
            )

    probe = df.limit(probe_rows).cache()
    dtypes = dict(probe.dtypes)
    n = probe.count()
    if n == 0:
        raise SchemaInferenceError("Input file has no rows.")
    card = probe.agg(*[F.approx_count_distinct(F.col(c)).alias(c) for c in df.columns]).first()
    distinct = {c: int(card[c]) for c in df.columns}

    def is_numeric(c: str) -> bool:
        t = dtypes[c]
        return t in NUMERIC_TYPES or t.startswith("decimal")

    # time
    time = ov.get("time")
    if time is None:
        cands = [c for c in df.columns if dtypes[c] in TIME_TYPES]
        if not cands:
            for c in (c for c in df.columns if dtypes[c] == "string"):
                stats = probe.agg(
                    F.count(c).alias("nn"), F.count(F.try_to_timestamp(F.col(c))).alias("ok")
                ).first()
                if stats["nn"] and stats["ok"] / stats["nn"] >= 0.95:
                    cands.append(c)
                    break
        time = cands[0] if cands else None

    taken = {time} if time else set()

    # key
    key = ov.get("key")
    if key is None:
        strs = [c for c in df.columns if dtypes[c] == "string" and c not in taken and distinct[c] > 1]
        ints = [c for c in df.columns if dtypes[c] in {"int", "bigint"} and c not in taken and distinct[c] > 1]
        pool = strs or ints
        if not pool:
            raise SchemaInferenceError("No string/integer column usable as key; set columns.key.")
        pool.sort(key=lambda c: (("id" in c.split("_")) or c.endswith("id"), distinct[c]), reverse=True)
        key = pool[0]
    taken.add(key)

    # categories
    cat = ov.get("category")
    cat2 = ov.get("category2")
    if cat is None:
        cats = sorted(
            (c for c in df.columns if dtypes[c] == "string" and c not in taken and distinct[c] >= 2),
            key=lambda c: distinct[c],
        )
        if not cats:
            raise SchemaInferenceError("No categorical string column found; set columns.category.")
        cat = cats[0]
        if cat2 is None and len(cats) > 1:
            cat2 = cats[1]
    taken.add(cat)
    if cat2:
        taken.add(cat2)

    # measures
    meas = ov.get("measure")
    meas2 = ov.get("measure2")
    if meas is None:
        nums = sorted(
            (c for c in df.columns if is_numeric(c) and c not in taken and distinct[c] > 1),
            key=lambda c: distinct[c],
            reverse=True,
        )
        if not nums:
            raise SchemaInferenceError("No numeric measure column found; set columns.measure.")
        meas = nums[0]
        if meas2 is None and len(nums) > 1:
            meas2 = nums[1]
    probe.unpersist()
    return ColumnRoles(key=key, category=cat, measure=meas, time=time, category2=cat2, measure2=meas2)


def write_schema_json(df: "DataFrame", roles: ColumnRoles, original_columns: Iterable[str],
                      path: Path) -> None:
    """Persist the real schema + inferred roles so the run is auditable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "columns": [{"name": n, "type": t} for n, t in df.dtypes],
        "original_column_names": list(original_columns),
        "roles": roles.as_dict(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Checksums  (format identical to `sha256sum`: "<hex>  <relative/path>")
# --------------------------------------------------------------------------- #
def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def load_checksums(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        digest, _, rel = line.partition("  ")
        out[rel.strip()] = digest.strip()
    return out


def save_checksums(path: Path, entries: dict[str, str]) -> None:
    header = (
        "# SHA-256 of downloaded raw files. Verify with: sha256sum -c data/checksums.sha256\n"
        "# Regenerate with: python scripts/download_data.py --write-checksums\n"
    )
    body = "".join(f"{d}  {rel}\n" for rel, d in sorted(entries.items()))
    path.write_text(header + body, encoding="utf-8")


def verify_files(files: Iterable[Path], base: Path, known: dict[str, str]) -> tuple[list[str], list[str], dict[str, str]]:
    """Compare files against ``known``.

    Returns ``(mismatched, unverified, computed)`` where ``unverified`` are files with
    no recorded digest and ``computed`` maps every file to its fresh digest.
    """
    mismatched, unverified, computed = [], [], {}
    for f in files:
        rel = f.relative_to(base).as_posix()
        digest = sha256_file(f)
        computed[rel] = digest
        if rel not in known:
            unverified.append(rel)
        elif known[rel] != digest:
            mismatched.append(rel)
    return mismatched, unverified, computed
