# Data

Nothing in this folder is the study dataset itself. The raw data belongs to its original
Kaggle authors, is governed by their licences, and is far too large for Git (the paper's table
is 120 GB uncompressed). The repository therefore ships:

| Path | What it is | In Git? |
|---|---|---|
| `data/raw/<source>/` | Files downloaded by `scripts/download_data.py` | No (git-ignored) |
| `data/sample/sample.csv` | Seeded random sample, <= 5 MB, made by `scripts/make_sample.py` | Yes, once generated |
| `data/checksums.sha256` | SHA-256 of every downloaded raw file (`sha256sum -c` compatible) | Yes |
| `data/schema/*.json` | Real schema + inferred column roles from the last run | No |
| `data/lake/` | Converted Parquet / Delta output | No |

## Sources (paper references [10] and [11])

1. Mahmood, I. (2024). *Tech & AI Companies Market Data 2024.* Kaggle dataset.
   <https://www.kaggle.com/datasets/imaadmahmood/tech-and-ai-companies-market-data-2024>
2. Obeidat, M. (2023). *Customer Segmentation with PySpark.* Kaggle notebook (source code, not data).
   <https://www.kaggle.com/code/mohammedobeidat/customer-segmentation-with-pyspark>

Check each source's licence/terms on its Kaggle page before redistributing anything. Do not commit
raw files to a public repository unless the licence allows it.

## Get the data

```bash
# 1. API token: https://www.kaggle.com/settings -> "Create New Token"
mkdir -p ~/.kaggle && mv ~/Downloads/kaggle.json ~/.kaggle/ && chmod 600 ~/.kaggle/kaggle.json

# 2. Download and verify
python scripts/download_data.py

# 3. FIRST TIME ONLY (maintainer): record digests, then commit data/checksums.sha256
python scripts/download_data.py --skip-download --write-checksums

# 4. Build the <= 5 MB sample that is committed for quick runs
python scripts/make_sample.py
```

`data/checksums.sha256` currently contains no digests: they can only be computed after the first
real download. After step 3, anyone who downloads the data gets an automatic integrity check, and
`--strict` makes a missing digest a hard failure (use it in CI).

## Schema is read from the files, not assumed

`scripts/convert_formats.py` reads the CSV header, normalises names to `snake_case` (Parquet/Delta
reject spaces and `,;{}()=`), infers column types, and picks the roles the three workloads need
(time, key, category, measure) from the real schema. The result is written to
`data/schema/<run>.json` for audit. To override any role, set it under `columns:` in `config.yaml`.

## TODO for the authors before publishing (not something code can decide)

The paper describes a 150-million-row, 120 GB table. State here exactly how that table was obtained
from the sources above (for example: downloaded as-is, joined from several files, or replicated /
scaled up from the Kaggle data). Reviewers will ask. Until this is written, the repository only
supports the **sample** and the smaller public files, and the measured cluster numbers in
`results/measured/` cannot be tied to a downloadable input.
