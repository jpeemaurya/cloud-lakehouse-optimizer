"""Typed access to ``config.yaml`` plus global seeding."""
from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_NAME = "config.yaml"


@dataclass(frozen=True)
class Paths:
    raw_dir: Path
    sample_dir: Path
    lake_dir: Path
    schema_dir: Path
    results_dir: Path
    checksums: Path


@dataclass(frozen=True)
class KaggleSource:
    name: str
    kind: str  # "dataset" | "kernel"
    slug: str


@dataclass(frozen=True)
class Config:
    root: Path
    seed: int
    paths: Paths
    sources: tuple[KaggleSource, ...]
    data: dict[str, Any]
    columns: dict[str, str | None]
    spark: dict[str, Any]
    formats: dict[str, dict[str, Any]]
    workloads: dict[str, dict[str, Any]]
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    def source(self, name: str) -> KaggleSource:
        for s in self.sources:
            if s.name == name:
                return s
        raise KeyError(f"Unknown source '{name}'. Known: {[s.name for s in self.sources]}")


def find_repo_root(start: Path | None = None) -> Path:
    """Walk upwards from ``start`` (default: this file) until config.yaml is found."""
    here = (start or Path(__file__)).resolve()
    for parent in [here, *here.parents]:
        if (parent / DEFAULT_CONFIG_NAME).is_file():
            return parent
    raise FileNotFoundError(f"Could not locate {DEFAULT_CONFIG_NAME} above {here}")


def load_config(path: str | os.PathLike[str] | None = None) -> Config:
    """Load ``config.yaml`` (or ``path``) and resolve all relative paths."""
    cfg_path = Path(path).resolve() if path else find_repo_root() / DEFAULT_CONFIG_NAME
    with open(cfg_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    root = cfg_path.parent

    p = raw["paths"]
    paths = Paths(**{k: (root / v) for k, v in p.items()})
    sources = tuple(KaggleSource(**s) for s in raw["kaggle"]["sources"])
    return Config(
        root=root,
        seed=int(raw.get("seed", 42)),
        paths=paths,
        sources=sources,
        data=raw["data"],
        columns=raw.get("columns", {}),
        spark=raw["spark"],
        formats=raw["formats"],
        workloads=raw["workloads"],
        raw=raw,
    )


def set_seeds(seed: int) -> None:
    """Seed Python and (if installed) NumPy so every run is repeatable."""
    random.seed(seed)
    os.environ.setdefault("PYTHONHASHSEED", str(seed))
    try:
        import numpy as np

        np.random.seed(seed)
    except ImportError:  # numpy is optional at this stage
        pass
