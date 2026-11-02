"""Pure-SQL tests (no Spark needed) plus end-to-end checks on a seeded fixture."""
import pytest

from lakehouse.data_io import ColumnRoles
from lakehouse.workloads import (
    build_dimension_sql, build_groupby, build_join_window, build_point_filter, sql_literal,
)

R = ColumnRoles(key="k", category="c", measure="m", time="t", category2="c2", measure2="m2")


def test_sql_literal_escapes():
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal(5) == "5"
    assert sql_literal(None) == "NULL"


def test_query_a_is_point_filter():
    sql = build_point_filter("fact", R, "X1")
    assert sql == "SELECT * FROM fact WHERE `k` = 'X1'"


def test_query_b_is_multidimensional_groupby():
    sql = build_groupby("fact", R, "month")
    assert "date_trunc('month', `t`)" in sql and "`c`" in sql and "`c2`" in sql
    assert "GROUP BY 1, 2, 3" in sql
    assert "SUM(`m`)" in sql


def test_query_b_without_time_or_second_category():
    r = ColumnRoles(key="k", category="c", measure="m")
    sql = build_groupby("fact", r)
    assert "date_trunc" not in sql and "GROUP BY 1" in sql


def test_query_c_has_join_and_window():
    sql = build_join_window("fact", "dim", R, top_n=5)
    assert "JOIN dim d ON" in sql
    assert "SUM(f.`m`) OVER (PARTITION BY f.`c`" in sql
    assert "RANK() OVER" in sql and "measure_rank <= 5" in sql
    assert "first_seen" in build_dimension_sql("fact", R)


@pytest.mark.spark
def test_end_to_end_on_fixture(spark, fixture_csv, cfg):
    from lakehouse.data_io import infer_roles, read_csv
    from lakehouse.workloads import build_queries, register_views, time_query

    df = read_csv(spark, fixture_csv)
    roles = infer_roles(df, {})
    assert roles.key == "order_id"
    assert roles.time == "event_time"
    assert roles.measure == "amount_usd"
    register_views(df, roles)
    qs = build_queries(df, roles, cfg.workloads)
    for qid, sql in qs.items():
        secs, n = time_query(spark, sql)
        assert secs >= 0 and n > 0, qid
