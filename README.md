# AirflowDeltaIgnite  
**A containerized data engineering platform with Apache Airflow, Spark, and Delta Lake**

![AirflowDeltaIgnite](https://img.shields.io/badge/Airflow-2.10.2-blue)  
![Spark](https://img.shields.io/badge/Spark-3.5.7-green)  
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.3.2-purple)  
![Python](https://img.shields.io/badge/Python-3.11-yellow)  
![ARM64 Native](https://img.shields.io/badge/ARM64-Native-success)

---

## Overview

**AirflowDeltaIgnite** is a fully containerized, modular data engineering platform designed for local development on **macOS (Apple Silicon)**. It integrates:

- **Apache Airflow 2.10.2** – Orchestrates ETL workflows via DAGs.
- **Apache Spark 3.5.7** – Distributed processing with standalone cluster.
- **Delta Lake 3.3.2** – ACID-compliant data lake with schema enforcement and time travel.
- **Jupyter Lab** – Interactive PySpark/Delta exploration.

Built for **multi-project ETL pipelines** (e.g., Divvy Bikes, Brewery), it follows the **Medallion architecture** (raw → bronze → silver → gold) with data quality checks and real-time API ingestion.

---

## Key Features

| Feature | Description |
|-------|-----------|
| **Medallion Architecture** | Layered data processing: raw (API landing), bronze (partitioned), silver (cleaned), gold (aggregated). |
| **Multi-Project Support** | Modular structure for multiple datasets in one Airflow instance. |
| **ARM64 Native** | Optimized for Apple Silicon (M1/M2/M3) – no emulation. |
| **Log Reduction** | Spark logs set to `WARN`, Ivy/NativeCodeLoader warnings suppressed. |
| **Automated Setup** | Spark connection (`spark_conn`) created at Airflow init. |
| **Pre-installed Delta JARs** | No runtime downloads – faster, cleaner logs. |
| **Jupyter Lab** | Interactive PySpark/Delta notebooks at `localhost:8888`. |

---

## Project Structure

```
spark-delta-cluster/
├── .env                    # Env vars: SPARK_VERSION=3.5.7, DELTA_VERSION=3.3.2, AIRFLOW_VERSION=2.10.2, PYTHON_VERSION=3.11, AIRFLOW_UID=501
│
├── Dockerfile              # Spark + Delta + Jupyter (Python 3.11, ARM64)
│
├── Dockerfile.airflow      # Airflow + Spark provider (Python 3.11, ARM64)
│
├── docker-compose.yml      # Defines services, volumes, spark-airflow-net network
│
├── entrypoint.sh           # Spark startup script (master/worker/history)
│
├── requirements.txt        # Airflow deps: apache-airflow==2.10.2, apache-airflow-providers-apache-spark==4.11.0, pyspark, delta-spark
│
├── README.md               # Setup, running instructions, adding projects
│
├── conf/                   # Spark configurations
│   ├── spark-defaults.conf # Spark settings (log4j WARN, Delta extensions)
│   └── log4j2.properties   # Custom Log4j (WARN, suppresses Ivy/NativeCodeLoader)
│
│   
├── data/                   # Persistent Delta Tables (raw/bronze/silver/gold)
│   │
│   ├── raw_data/           # API landing (e.g., divvy_bikes/free_bike_status/)
│   │   ├── divvy_bikes/
│   │   └── brewery/
│   │
│   └── warehouse/
│       │
│       ├── bronze.db/
│       │   └── divvy_bikes/
│       │
│       ├── silver.db/
│       │   └── divvy_bikes_status/
│       │   └── divvy_station_information/
│       │   └── divvy_station_status/
│       │   └── divvy_system_pricing_plan/
│       │   └── divvy_vehicle_types/
│       │
│       └── gold.db/
│           └── divvy_bikes_status/
│           └── divvy_station_information/
│           └── divvy_station_status/
│           └── divvy_system_pricing_plan/
│           └── divvy_vehicle_types/
│
├── dags/                   # Airflow DAGs
│   ├── __init__.py         # Makes dags a Python package
│   ├── common/
│   ├── divvy_bikes/
│   └── brewery/
│
├── etl/                    # ETL Python code
│   ├── common/
│   │   ├── __init__.py
│   │   ├── utils.py        # Spark session, data quality checks
│   │   └── DeltaSpark.py  # Shared transforms (e.g., dedup)
│   │
│   ├── divvy_bikes/
│   │   ├── transformations/
│   │   │   ├── __init__.py
│   │   │   ├── bronze.py
│   │   │   ├── bronze_to_delta.py
│   │   │   ├── gold.py
│   │   │   ├── gold_to_delta.py
│   │   │   ├── silver.py
│   │   │   └── silver_to_delta.py
│   │   │
│   │   ├── apis/
│   │   │   ├── __init__.py
│   │   │   └── divvy_api.py   # API fetching (requests)
│   │   │
│   │   └── ustils/
│   │       ├── __init__.py
│   │       ├── ClassesCall.py
│   │       └── Paths.py
│   │
│   └── brewery/
│       ├── transformations/
│       │   ├── __init__.py
│       │   ├── bronze.py
│       │   ├── bronze_to_delta.py
│       │   ├── gold.py
│       │   ├── gold_to_delta.py
│       │   ├── silver.py
│       │   └── silver_to_delta.py
│       │
│       ├── apis/
│       │   ├── __init__.py
│       │   └── brewery_api.py   # API fetching (requests)
│       │
│       └── ustils/
│           ├── __init__.py
│           ├── ClassesCall.py
│           └── Paths.py
│
│   
├── tests/                  # Unit/integration tests (pytest)
│   ├── common/
│   │   └── test_utils.py
│   ├── divvy_bikes/
│   │   └── test_etl.py
│   └── brewery/
│       └── test_etl.py
│
├── airflow-logs/           # Airflow/Spark log redirection
│
└── notebooks/             # Jupyter notebooks for ad-hoc analysis
```

---

## Airflow + Spark + Delta Lake Integration

### How It Works

1. **Airflow DAGs** (`dags/`) use `SparkSubmitOperator` to submit PySpark jobs.
2. **Spark Cluster** (`spark-master`, `spark-worker`) runs in client mode:
   - Driver: Airflow scheduler container
   - Executors: Spark worker(s)
3. **Delta Lake** enables:
   - ACID transactions
   - Schema enforcement
   - Time travel (`VERSION AS OF`)
4. **ETL Scripts** (`etl/`) use shared `get_spark_session()` with Delta configs.
5. **Data Quality** checks run in silver layer (nulls, uniqueness).
6. **Logs** are minimized via:
   - `verbose=False` in `SparkSubmitOperator`
   - Log4j `WARN` level
   - Pre-installed Delta JARs (no Ivy spam)

---

## Getting Started

### Prerequisites
- Docker Desktop (with Rosetta for ARM64)
- macOS (Apple Silicon recommended)

### Quick Start

```bash
# Clone and start
git clone https://github.com/yourusername/AirflowDeltaIgnite.git
cd AirflowDeltaIgnite
docker compose up --build -d

# Access UIs
- Airflow: http://localhost:8082 (airflow/airflow)
- Spark Master: http://localhost:8080
- Jupyter: http://localhost:8888
```

### Run a DAG
1. Go to Airflow UI → DAGs → `divvy_bikes_ingestion`
2. Trigger → Check logs and `./data` for Delta tables.

---

## Adding a New Project

1. Duplicate `dags/divvy_bikes/` → `dags/new_project/`
2. Duplicate `etl/divvy_bikes/` → `etl/new_project/`
3. Add config in `config/new_project_config.yaml`
4. Restart Airflow: `docker compose restart airflow-scheduler`

---

## Examples

### Divvy Bikes
<img width="2477" height="666" alt="image" src="https://github.com/user-attachments/assets/6d330514-761a-4ce7-8b01-0270dd6d9928" />

#### Extract
<img width="1124" height="577" alt="image" src="https://github.com/user-attachments/assets/c5e175c7-48d7-4331-8336-3edb615c70d8" />

#### Bronze
<img width="1114" height="854" alt="image" src="https://github.com/user-attachments/assets/89b15ecf-ac86-4a0b-89af-e232b0f0e5c8" />

#### Silver
<img width="1002" height="824" alt="image" src="https://github.com/user-attachments/assets/6ad91df3-db89-45be-9421-b9dfea1d350b" />

#### Gold
<img width="1002" height="758" alt="image" src="https://github.com/user-attachments/assets/26df5a86-4551-4566-915d-0e3074894907" />

---

## License

MIT

---

**AirflowDeltaIgnite** – Ignite your data pipelines with Airflow, Spark, and Delta Lake.
