from pathlib import Path

from lakehouse.config import load_config, set_seeds
from lakehouse.data_io import (
    load_checksums, normalise_name, save_checksums, sha256_file, verify_files,
)


def test_config_loads_and_resolves_paths():
    cfg = load_config()
    assert cfg.seed == 42
    assert cfg.paths.raw_dir.is_absolute()
    assert cfg.source("tech_ai_companies_market_data_2024").kind == "dataset"
    assert cfg.formats["parquet_zstd"]["codec"] == "zstd"


def test_set_seeds_is_repeatable():
    import random

    set_seeds(7)
    a = random.random()
    set_seeds(7)
    assert a == random.random()


def test_normalise_name():
    assert normalise_name("Amount (USD)") == "amount_usd"
    assert normalise_name("Order ID") == "order_id"
    assert normalise_name("1st col") == "c_1st_col"


def test_checksum_roundtrip(tmp_path: Path):
    base = tmp_path / "raw"
    (base / "s").mkdir(parents=True)
    f = base / "s" / "a.csv"
    f.write_text("x,y\n1,2\n")
    digest = sha256_file(f)
    ck = tmp_path / "checksums.sha256"
    save_checksums(ck, {"s/a.csv": digest})
    known = load_checksums(ck)
    assert known == {"s/a.csv": digest}

    mism, unv, _ = verify_files([f], base, known)
    assert mism == [] and unv == []

    f.write_text("x,y\n1,3\n")  # tamper
    mism, _, _ = verify_files([f], base, known)
    assert mism == ["s/a.csv"]

    g = base / "s" / "b.csv"
    g.write_text("q\n")
    _, unv, _ = verify_files([g], base, known)
    assert unv == ["s/b.csv"]
