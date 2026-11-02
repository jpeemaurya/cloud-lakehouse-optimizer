"""Shared fixtures. The CSV below is a *test fixture only* (tiny, random, seeded); it is not
the study dataset and is never shipped in data/."""
from __future__ import annotations

import os
import random
import shutil
from pathlib import Path

import pytest

from lakehouse.config import load_config  # noqa: E402
from lakehouse.storage_formats import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def cfg():
    return load_config()


@pytest.fixture(scope="session")
def fixture_csv(tmp_path_factory) -> Path:
    rng = random.Random(0)
    p = tmp_path_factory.mktemp("fx") / "fixture.csv"
    cats = ["alpha", "beta", "gamma", "delta"]
    regions = ["n", "s", "e", "w", "c", "x", "y"]
    with open(p, "w") as fh:
        fh.write("Order ID,Event Time,Segment,Region,Amount (USD),Quantity\n")
        for i in range(2000):
            fh.write(
                f"ORD{i % 500:04d},2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d} 10:00:00,"
                f"{rng.choice(cats)},{rng.choice(regions)},{rng.random() * 1000:.4f},{rng.randint(1, 50)}\n"
            )
    return p


@pytest.fixture(scope="session")
def spark(cfg, tmp_path_factory):
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:  # Spark needs a JVM
        pytest.skip("Java not found; Spark tests skipped")
    with_delta = os.environ.get("RUN_DELTA_TESTS") == "1"
    s = get_spark(cfg, with_delta=with_delta)
    s.conf.set("spark.sql.shuffle.partitions", "4")
    yield s
    s.stop()
