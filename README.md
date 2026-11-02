# cloud-lakehouse-optimizer
# Cloud Optimizer
### A Hierarchical Workload-Aware Optimization Framework for Cloud-Based Big Data Analytics Using Parquet and Delta Lake

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Platform](https://img.shields.io/badge/Platform-Google%20Colab-orange)
![Framework](https://img.shields.io/badge/Framework-Cloud%20Optimization-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## 1. Overview

Cloud Optimizer is a workload-aware optimization framework designed to improve the performance, storage efficiency, resource utilization, and operational cost of cloud-based big data analytics.

The framework evaluates different data storage formats, including CSV, Apache Parquet, and Delta Lake, under different analytical workloads. It analyzes execution time, storage consumption, CPU utilization, memory usage, I/O performance, and estimated cloud infrastructure costs.

The proposed framework incorporates a multi-objective optimization strategy to identify an appropriate storage format and configuration based on workload characteristics and performance requirements.

## 2. Key Objectives

- Optimize analytical query execution in cloud environments.
- Compare CSV, Apache Parquet, and Delta Lake storage formats.
- Evaluate workload-dependent performance characteristics.
- Minimize storage requirements and data processing overhead.
- Monitor CPU and memory utilization.
- Estimate cloud infrastructure and execution costs.
- Develop a workload-aware multi-objective optimization strategy.
- Identify suitable storage configurations for different analytical workloads.

## 3. System Architecture

The proposed Cloud Optimizer follows a hierarchical optimization architecture consisting of the following stages:

1. **Data Generation and Ingestion:** Generates or loads large-scale analytical datasets.
2. **Storage Layer:** Stores datasets using CSV, Parquet, and Delta Lake formats.
3. **Workload Generation:** Executes different analytical workloads.
4. **Performance Monitoring:** Measures execution time, CPU utilization, memory consumption, and storage requirements.
5. **Cost Modeling:** Estimates infrastructure and operational costs.
6. **Multi-Objective Optimization:** Evaluates alternative configurations using a weighted optimization function.
7. **Result Visualization:** Generates comparative graphs, tables, and performance reports.

## 4. Major Features

| Feature | Description |
|---|---|
| Storage Optimization | Comparison of CSV, Parquet, and Delta Lake |
| Workload Analysis | Evaluation of multiple analytical workloads |
| Performance Monitoring | Execution time, CPU, and memory |
| Storage Evaluation | Storage consumption and compression efficiency |
| Cost Estimation | Modeled cloud infrastructure cost |
| Multi-objective Optimization | Weighted performance and cost evaluation |
| Visualization | Comparative performance graphs |
| Report Generation | CSV and Excel-based results |

## 5. Technologies and Tools

The framework utilizes the following technologies:

- **Programming Language:** Python
- **Development Environment:** Google Colab / Jupyter Notebook
- **Data Processing:** Pandas, NumPy
- **Columnar Storage:** Apache Parquet, PyArrow
- **Query Processing:** DuckDB
- **Transactional Storage:** Delta Lake
- **Resource Monitoring:** psutil
- **Visualization:** Matplotlib
- **Report Generation:** OpenPyXL

## 6. Workload Evaluation

The framework supports multiple analytical workload categories:

- Selective filtering and data retrieval.
- Aggregation and group-by operations.
- Complex analytical queries and joins.
- Different workload intensity levels.
- Repeated query execution for performance measurement.

Each workload is evaluated across different storage configurations to identify performance variations.

## 7. Optimization Methodology

The proposed framework employs a weighted multi-objective optimization approach.

The optimization objective can be represented as:

J = αT + βC + γR + δS

Where:

- J = Overall optimization objective.
- T = Normalized query execution time.
- C = Normalized cloud execution cost.
- R = Normalized resource utilization.
- S = Normalized storage consumption.
- α, β, γ, δ = Weighting coefficients.

The framework evaluates alternative configurations and identifies the configuration with the minimum objective value.

The weighting coefficients can be adjusted according to application requirements and optimization priorities.

## 8. Installation and Requirements

Install the required Python libraries:

```bash
pip install pandas numpy pyarrow duckdb deltalake psutil matplotlib openpyxl
```

For Google Colab:

1. Open Google Colab.
2. Upload the project notebook.
3. Install the required dependencies.
4. Execute the notebook cells sequentially.
5. Review the generated performance and optimization reports.

## 9. Execution

Run the project notebook in Google Colab or Jupyter Notebook.

The general execution workflow is:

```text
Input Dataset
      |
      v
Data Preprocessing
      |
      v
Storage Format Generation
(CSV / Parquet / Delta Lake)
      |
      v
Workload Generation
      |
      v
Performance Evaluation
      |
      v
Resource and Cost Modeling
      |
      v
Multi-Objective Optimization
      |
      v
Optimal Configuration Selection
      |
      v
Visualization and Reports
```

## 10. Performance Evaluation Parameters

The framework considers the following evaluation parameters:

| Parameter | Evaluation Purpose |
|---|---|
| Execution Time | Query processing efficiency |
| CPU Utilization | Computational resource consumption |
| Memory Utilization | Memory efficiency |
| Storage Consumption | Storage optimization |
| I/O Throughput | Data transfer efficiency |
| Cloud Cost | Estimated operational expenditure |
| Workload Intensity | Workload-dependent performance |

## 11. Experimental Scope

The framework is designed to investigate cloud-based big data analytics involving large-scale datasets and distributed processing environments.

The experimental study considers different storage formats, analytical workloads, and infrastructure configurations.

The framework can be extended to evaluate different cluster sizes, workload intensities, compression techniques, and cloud deployment configurations.

## 12. Expected Outputs

The execution generates:

- Comparative storage performance results.
- Query execution time analysis.
- CPU and memory utilization reports.
- Storage efficiency comparisons.
- Estimated cloud cost analysis.
- Multi-objective optimization results.
- Performance visualization graphs.
- CSV and Excel-based experimental reports.

## 13. Research Applications

The proposed Cloud Optimizer can support research and experimentation in:

- Cloud Computing.
- Big Data Analytics.
- Cloud Storage Optimization.
- Workload-Aware Resource Management.
- Cost-Aware Cloud Computing.
- Data Engineering.
- Performance and Energy Efficiency Evaluation.

## 14. Future Enhancements

Potential extensions include:

- Machine learning-based workload prediction.
- Intelligent cloud resource allocation.
- Automated cloud autoscaling.
- Real-time workload monitoring.
- Multi-cloud performance optimization.
- Energy-aware cloud resource management.
- Integration with AWS, Microsoft Azure, and Google Cloud.

## ## 15. Repository Structure

The repository is organized into configuration files, data management utilities, core Lakehouse implementation modules, experimental results, Jupyter notebooks, automated tests, and continuous integration workflows.

```text
cloud-lakehouse-optimizer/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   ├── README_DATA.md
│   ├── checksums.sha256
│   ├── raw/
│   │   └── .gitkeep
│   └── sample/
│       └── README.md
│
├── figures/
│   └── .gitkeep
│
├── notebooks/
│   └── 01_inspect_schema.ipynb
│
├── results/
│   ├── measured/
│   │   ├── README.md
│   │   ├── table1_storage_footprint.csv
│   │   ├── table2_query_latency_seconds.csv
│   │   ├── table3_io_shuffle_cpu.csv
│   │   └── table4_tco_energy.csv
│   │
│   └── simulated/
│       └── .gitkeep
│
├── scripts/
│   ├── convert_formats.py
│   ├── download_data.py
│   └── make_sample.py
│
├── src/
│   └── lakehouse/
│       ├── __init__.py
│       ├── config.py
│       ├── data_io.py
│       ├── storage_formats.py
│       └── workloads.py
│
├── tests/
│   ├── conftest.py
│   ├── test_config_and_checksums.py
│   ├── test_storage_formats.py
│   └── test_workloads.py
│
├── .gitattributes
├── .gitignore
├── CITATION.cff
├── LICENSE
├── Makefile
├── README.md
├── config.yaml
├── environment.yml
├── pyproject.toml
└── requirements.txt
```

### Directory and File Description

| Directory / File | Description |
|---|---|
| `.github/workflows/ci.yml` | GitHub Actions workflow for automated testing and continuous integration. |
| `data/` | Contains dataset documentation, checksum information, and sample/raw data directories. |
| `data/README_DATA.md` | Explains dataset sources, acquisition procedures, licensing, and data preparation. |
| `data/checksums.sha256` | Stores SHA-256 checksums for downloaded dataset files. |
| `data/raw/` | Reserved for original datasets downloaded from Kaggle. |
| `data/sample/` | Reserved for the generated sample dataset used in local experiments. |
| `figures/` | Reserved for experimental figures and graphical representations. |
| `notebooks/01_inspect_schema.ipynb` | Jupyter notebook for inspecting inferred dataset schemas, column roles, and experimental results. |
| `results/measured/` | Contains experimental measurements transcribed from the research manuscript. |
| `results/measured/table1_storage_footprint.csv` | Storage footprint and compression comparison. |
| `results/measured/table2_query_latency_seconds.csv` | Query execution latency for different workloads and cluster configurations. |
| `results/measured/table3_io_shuffle_cpu.csv` | I/O throughput, shuffle, spill, and CPU utilization measurements. |
| `results/measured/table4_tco_energy.csv` | Total cost of ownership (TCO) and energy consumption measurements. |
| `results/simulated/` | Destination for results generated by local format-conversion experiments. |
| `scripts/convert_formats.py` | Converts input CSV datasets into Parquet (Snappy/Zstandard) and Delta Lake formats and records storage measurements. |
| `scripts/download_data.py` | Downloads configured Kaggle datasets/notebooks and verifies their SHA-256 checksums. |
| `scripts/make_sample.py` | Generates a reproducible, size-limited sample dataset from the downloaded CSV data. |
| `src/lakehouse/__init__.py` | Initializes the Lakehouse Python package. |
| `src/lakehouse/config.py` | Loads project configuration, resolves directory paths, and manages reproducible random seeds. |
| `src/lakehouse/data_io.py` | Handles CSV ingestion, schema inspection, column-role inference, and checksum verification. |
| `src/lakehouse/storage_formats.py` | Implements Spark session initialization and storage-format generation for CSV, Parquet, and Delta Lake. |
| `src/lakehouse/workloads.py` | Defines analytical workloads and query execution for performance evaluation. |
| `tests/conftest.py` | Provides shared test fixtures and testing configurations. |
| `tests/test_config_and_checksums.py` | Tests configuration loading and checksum-related functionality. |
| `tests/test_storage_formats.py` | Validates storage-format implementation and related operations. |
| `tests/test_workloads.py` | Tests analytical workload generation and execution functionality. |
| `.gitattributes` | Defines Git file-handling attributes. |
| `.gitignore` | Specifies generated files, temporary data, and local artifacts excluded from version control. |
| `CITATION.cff` | Provides software citation metadata for academic referencing. |
| `LICENSE` | Contains the project's MIT License terms. |
| `Makefile` | Provides command-line shortcuts for installation, downloading data, sampling, conversion, testing, and cleanup. |
| `config.yaml` | Central configuration for datasets, directory paths, Spark settings, storage formats, and analytical workloads. |
| `environment.yml` | Defines the Conda environment, Python version, Java dependency, and package installation. |
| `pyproject.toml` | Defines project metadata, package configuration, dependencies, and pytest settings. |
| `requirements.txt` | Lists Python dependencies required to execute the framework. |
| `README.md` | Provides project overview, methodology, installation, execution, and documentation. |

**Note:** Directories such as `data/lake/`, `data/schema/`, and generated CSV files under `results/simulated/` are created during execution and are not currently part of the committed repository tree. The `.gitkeep` files preserve otherwise empty directories.
## 16. License

This project is intended for academic research, experimentation, and educational purposes. Specify the applicable open-source license in the repository before redistribution.

## 17. Acknowledgement

This research-oriented implementation explores workload-aware optimization strategies for cloud-based big data analytics, with particular emphasis on storage efficiency, analytical performance, resource utilization, and cost optimization.

---

**Project:** Cloud Optimizer  
**Research Area:** Cloud Computing and Big Data Analytics  
**Core Approach:** Hierarchical Workload-Aware Multi-Objective Optimization
