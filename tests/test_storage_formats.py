import os

import pytest

from lakehouse.data_io import infer_roles, read_csv
from lakehouse.storage_formats import data_size_bytes, write_delta, write_parquet


def test_parquet_snappy_and_zstd_roundtrip(spark, fixture_csv, tmp_path):
    df = read_csv(spark, fixture_csv)
    n = df.count()
    for codec in ("snappy", "zstd"):
        res = write_parquet(df, tmp_path / codec, codec)
        assert res.size_gb > 0
        assert spark.read.parquet(str(tmp_path / codec)).count() == n
        assert data_size_bytes(tmp_path / codec) > 0


@pytest.mark.skipif(os.environ.get("RUN_DELTA_TESTS") != "1",
                    reason="set RUN_DELTA_TESTS=1 (needs delta-spark jars from Maven)")
@pytest.mark.parametrize("layout", ["liquid", "zorder"])
def test_delta_layouts(spark, fixture_csv, cfg, tmp_path, layout):
    from dataclasses import replace

    df = read_csv(spark, fixture_csv)
    roles = infer_roles(df, {})
    fmts = {**cfg.formats, "delta": {**cfg.formats["delta"], "layout": layout}}
    cfg2 = replace(cfg, formats=fmts)
    res = write_delta(df, tmp_path / layout, cfg2, roles, table_name=f"lakehouse.t_{layout}")
    assert res.size_gb > 0
    assert spark.read.format("delta").load(str(tmp_path / layout)).count() == df.count()
