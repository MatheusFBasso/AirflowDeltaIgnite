AirflowDeltaIgnite is a containerized data engineering platform using Docker Compose, integrating Apache Airflow 2.10.2 for orchestration, Apache Spark 3.5.7 with Delta Lake 3.3.2 for data lakes, and Jupyter Lab for PySpark development. Optimized for macOS (native ARM64), it supports multi-project ETL pipelines (e.g., Divvy Bikes, Brewery) with Medallion architecture (raw/bronze/silver/gold layers), API ingestion, and data quality checks. Features include automated Spark connection setup, minimized logging (WARN level, suppressed Ivy warnings), and modular code for scalability.

#### Scope
- **Core Components**: Spark cluster for distributed processing; Airflow DAGs for ETL; Jupyter for exploration.
- **Medallion Layers**: Raw (API landing), Bronze (partitioned Delta), Silver (cleaned/validated), Gold (aggregated insights).
- **Multi-Project**: Modular folders for isolation and shared utils.
- **UIs**: Spark (localhost:8080), Airflow (8082), Jupyter (8888).
- **Limitations**: Dev-focused; standalone Spark; ARM64-optimized.
- **Use Cases**: Real-time API workflows for transportation/environmental data.

#### Structure
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
