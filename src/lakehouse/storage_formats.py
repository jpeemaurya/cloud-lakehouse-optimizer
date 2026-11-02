"""Spark session factory and writers for the three storage layouts compared in the paper:
row-oriented CSV, Apache Parquet (Snappy / Zstandard) and Delta Lake with Liquid
Clustering (or Z-Order as a fallback)."""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .config import Config
from .data_io import ColumnRoles

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

BYTES_PER_GB = 1024 ** 3


@dataclass
class WriteResult:
    fmt: str
    codec: str
    path: Path
    size_gb: float
    seconds: float


def get_spark(cfg: Config, with_delta: bool = True) -> "SparkSession":
    """Create a local Spark 3.5 session; enable Delta Lake when ``delta-spark`` is installed."""
    from pyspark.sql import SparkSession

    s = cfg.spark
    builder = (
        SparkSession.builder.appName(s["app_name"])
        .master(s["master"])
        .config("spark.driver.memory", s["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
        .config("spark.sql.warehouse.dir", str(cfg.paths.lake_dir / "_warehouse"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    for k, v in (s.get("extra_conf") or {}).items():
        builder = builder.config(k, str(v))

    if with_delta:
        try:
            from delta import configure_spark_with_delta_pip

            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:
            spark = builder.getOrCreate()
    else:
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def data_size_bytes(path: Path) -> int:
    """Bytes of data files below ``path`` (ignores ``_delta_log`` and Spark marker/crc files)."""
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    total = 0
    for f in path.rglob("*"):
        if not f.is_file():
            continue
        if "_delta_log" in f.parts or f.name.startswith(("_", ".")):
            continue
        total += f.stat().st_size
    return total


def write_parquet(df: "DataFrame", out: Path, codec: str) -> WriteResult:
    """Write ``df`` as Parquet with the given compression codec (snappy | zstd | ...)."""
    spark = df.sparkSession
    spark.conf.set("spark.sql.parquet.compression.codec", codec)
    t0 = time.perf_counter()
    df.write.mode("overwrite").parquet(str(out))
    dt = time.perf_counter() - t0
    return WriteResult("parquet", codec, out, data_size_bytes(out) / BYTES_PER_GB, dt)


def resolve_cluster_cols(cfg: Config, roles: ColumnRoles) -> list[str]:
    cols = list(cfg.formats["delta"].get("cluster_by") or [])
    if cols:
        return cols
    return [c for c in (roles.key, roles.time) if c]


def write_delta(df: "DataFrame", out: Path, cfg: Config, roles: ColumnRoles,
                table_name: str = "lakehouse.telemetry") -> WriteResult:
    """Write a Delta table.

    ``layout: liquid``  -> ``CLUSTER BY`` (Liquid Clustering, requires Delta >= 3.1) then OPTIMIZE.
    ``layout: zorder``  -> plain Delta table then ``OPTIMIZE ... ZORDER BY``.
    """
    from delta.tables import DeltaTable  # raises ImportError if delta-spark is missing

    spark = df.sparkSession
    dcfg = cfg.formats["delta"]
    spark.conf.set("spark.sql.parquet.compression.codec", dcfg.get("compression", "snappy"))
    cols = resolve_cluster_cols(cfg, roles)
    layout = dcfg.get("layout", "liquid")
    t0 = time.perf_counter()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.split('.')[0]}")
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    if layout == "liquid":
        (DeltaTable.create(spark).tableName(table_name).location(str(out))
         .addColumns(df.schema).clusterBy(*cols).execute())
        df.write.format("delta").mode("append").insertInto(table_name)
        spark.sql(f"OPTIMIZE {table_name}")
    elif layout == "zorder":
        df.write.format("delta").mode("overwrite").save(str(out))
        zcols = ", ".join(f"`{c}`" for c in cols)
        spark.sql(f"OPTIMIZE delta.`{out}` ZORDER BY ({zcols})")
        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{out}'")
    else:
        raise ValueError(f"formats.delta.layout must be 'liquid' or 'zorder', got {layout!r}")
    dt = time.perf_counter() - t0
    return WriteResult(f"delta-{layout}", dcfg.get("compression", "snappy"), out,
                       data_size_bytes(out) / BYTES_PER_GB, dt)
