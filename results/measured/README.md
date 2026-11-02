# Measured results (from the paper)

These four CSVs are transcribed directly from Tables 1-4 of the manuscript. They were measured on
AWS EMR (`r6i.4xlarge`, 4/16/32/64 nodes) and **cannot be regenerated without that cloud setup**;
they are provided as the reference values for fitting and comparison.

| File | Paper table | Content |
|---|---|---|
| `table1_storage_footprint.csv` | Table 1 | Storage format, compression, size (GB), ratio |
| `table2_query_latency_seconds.csv` | Table 2 | Latency (s) of Queries A/B/C by nodes and format |
| `table3_io_shuffle_cpu.csv` | Table 3 | Read throughput, shuffle, spill, CPU utilisation |
| `table4_tco_energy.csv` | Table 4 | Monthly TCO and energy per 10k queries |

Verify against the final manuscript before release: if a table changes in review, regenerate these.
