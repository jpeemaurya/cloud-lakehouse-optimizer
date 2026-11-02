.PHONY: install data checksums sample convert convert-sample test test-delta clean

install:
	pip install -r requirements.txt && pip install -e .

data:            ## download Kaggle sources + verify SHA-256
	python scripts/download_data.py

checksums:       ## record digests of the downloaded files (commit the result)
	python scripts/download_data.py --skip-download --write-checksums

sample:          ## build data/sample/sample.csv (<= 5 MB)
	python scripts/make_sample.py

convert:         ## full-size CSV -> Parquet (Snappy, Zstd) -> Delta (Liquid Clustering)
	python scripts/convert_formats.py

convert-sample:  ## same, on the 5 MB sample (laptop)
	python scripts/convert_formats.py --sample

test:
	pytest -q

test-delta:      ## also exercise Delta Lake (downloads jars from Maven on first run)
	RUN_DELTA_TESTS=1 pytest -q

clean:
	rm -rf data/lake data/schema spark-warehouse metastore_db derby.log .pytest_cache
