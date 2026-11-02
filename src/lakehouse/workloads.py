"""The three analytical workloads of the paper, expressed in Spark SQL.

* Query A - selective point filter (predicate pushdown / file pruning)
* Query B - multidimensional group-by aggregation (memory bandwidth, intermediate state)
* Query C - multi-table join with window operations (network shuffle, compute)

SQL builders are pure functions of the *resolved* column roles, so no column name is
hard-coded. The "dimension table" for Query C is derived from the fact table itself
(per-key aggregates), so the join needs no extra input file.
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from .data_io import ColumnRoles

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

QUERY_IDS = ("A", "B", "C")


def q(name: str) -> str:
    """Quote an identifier for Spark SQL."""
    return "`" + name.replace("`", "``") + "`"


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def build_dimension_sql(fact: str, r: ColumnRoles) -> str:
    """Per-key aggregate used as the second table of Query C."""
    first_seen = f", MIN({q(r.time)}) AS first_seen" if r.time else ""
    return (
        f"SELECT {q(r.key)} AS dim_key, COUNT(*) AS key_rows, "
        f"SUM({q(r.measure)}) AS key_total{first_seen} "
        f"FROM {fact} GROUP BY {q(r.key)}"
    )


def build_point_filter(fact: str, r: ColumnRoles, value: Any) -> str:
    """Query A."""
    return f"SELECT * FROM {fact} WHERE {q(r.key)} = {sql_literal(value)}"


def build_groupby(fact: str, r: ColumnRoles, grain: str = "month") -> str:
    """Query B: group by [time bucket,] category [, category2]."""
    dims, select = [], []
    if r.time:
        select.append(f"date_trunc('{grain}', {q(r.time)}) AS period")
        dims.append("period")
    for c in (r.category, r.category2):
        if c:
            select.append(q(c))
            dims.append(q(c))
    aggs = [
        "COUNT(*) AS n_rows",
        f"SUM({q(r.measure)}) AS sum_measure",
        f"AVG({q(r.measure)}) AS avg_measure",
        f"approx_count_distinct({q(r.key)}) AS n_keys",
    ]
    if r.measure2:
        aggs.append(f"MAX({q(r.measure2)}) AS max_measure2")
    group_pos = ", ".join(str(i + 1) for i in range(len(select)))
    return f"SELECT {', '.join(select + aggs)} FROM {fact} GROUP BY {group_pos}"


def build_join_window(fact: str, dim: str, r: ColumnRoles, top_n: int = 10) -> str:
    """Query C: fact JOIN per-key dimension, running total + rank window functions."""
    order_run = q(r.time) if r.time else q(r.measure)
    inner = (
        f"SELECT f.{q(r.key)} AS k, f.{q(r.category)} AS cat, f.{q(r.measure)} AS m, "
        f"d.key_total, d.key_rows, "
        f"SUM(f.{q(r.measure)}) OVER (PARTITION BY f.{q(r.category)} "
        f"ORDER BY f.{order_run} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total, "
        f"RANK() OVER (PARTITION BY f.{q(r.category)} ORDER BY f.{q(r.measure)} DESC) AS measure_rank "
        f"FROM {fact} f JOIN {dim} d ON f.{q(r.key)} = d.dim_key"
    )
    return f"SELECT * FROM ({inner}) WHERE measure_rank <= {int(top_n)}"


def pick_filter_value(df: "DataFrame", r: ColumnRoles) -> Any:
    """Deterministically choose a key with median row-frequency (a realistic selective filter)."""
    from pyspark.sql import functions as F

    counts = df.groupBy(r.key).count().where(F.col(r.key).isNotNull())
    median = counts.approxQuantile("count", [0.5], 0.01)[0]
    row = counts.where(F.col("count") >= median).orderBy("count", r.key).first()
    return row[r.key]


def register_views(df: "DataFrame", roles: ColumnRoles, fact: str = "fact", dim: str = "dim") -> None:
    """Expose ``df`` as ``fact`` and the derived per-key table as ``dim`` temp views."""
    spark = df.sparkSession
    df.createOrReplaceTempView(fact)
    spark.sql(build_dimension_sql(fact, roles)).createOrReplaceTempView(dim)


def build_queries(df: "DataFrame", roles: ColumnRoles, wl_cfg: dict[str, dict[str, Any]],
                  fact: str = "fact", dim: str = "dim") -> dict[str, str]:
    """Return ``{"A": sql, "B": sql, "C": sql}`` for the registered views."""
    value = wl_cfg["point_filter"].get("filter_value")
    if value is None:
        value = pick_filter_value(df, roles)
    return {
        "A": build_point_filter(fact, roles, value),
        "B": build_groupby(fact, roles, wl_cfg["groupby"].get("time_grain", "month")),
        "C": build_join_window(fact, dim, roles, wl_cfg["join_window"].get("top_n", 10)),
    }


def time_query(spark: "SparkSession", sql: str) -> tuple[float, int]:
    """Execute ``sql`` fully (noop sink, no output written) and return (seconds, row_count)."""
    df = spark.sql(sql)
    t0 = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    elapsed = time.perf_counter() - t0
    return elapsed, df.count()
