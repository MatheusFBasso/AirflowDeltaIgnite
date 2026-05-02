# AirflowDeltaIgnite
**A containerized data engineering platform with Apache Airflow, Spark, and Delta Lake**

![Airflow-2.10.2](https://img.shields.io/badge/Airflow-2.10.2-blue)
![Spark-4.0.1](https://img.shields.io/badge/Spark-4.0.1-green)
![Delta Lake-4.0.0](https://img.shields.io/badge/Delta_Lake-4.0.0-purple)
![Python-3.12](https://img.shields.io/badge/Python-3.12-yellow)

---

## Overview

**AirflowDeltaIgnite** is a fully containerized, modular data engineering platform designed for local development. It integrates:

- **Apache Airflow** to orchestrate ETL workflows.
- **Apache Spark** for distributed data processing.
- **Delta Lake** for reliable and ACID-compliant data storage.
- **Jupyter Lab** for interactive data exploration.

This platform is built to handle multi-project ETL pipelines using the Medallion architecture (raw → bronze → silver → gold), complete with data quality checks and real-time API ingestion.

---

## Key Features

| Feature | Description |
|---|---|
| **Medallion Architecture** | Layered data processing from raw landing to aggregated gold tables. |
| **Multi-Project Support** | Modular structure to support multiple, isolated datasets in one Airflow instance. |
| **Cross-Platform** | Runs on any system with Docker, including Windows, macOS, and Linux. |
| **Automated Setup** | The Spark connection is automatically created when Airflow initializes. |
| **Pre-installed Dependencies** | Delta JARs are pre-installed to ensure faster startup and cleaner logs. |
| **Jupyter Lab Integration** | Interactive PySpark and Delta notebooks available at `localhost:8888`. |

---

## Project Structure

```
AirflowDeltaIgnite/
├── conf/                   # Spark configurations (log4j, defaults)
├── dags/                   # Airflow DAGs for each project
├── data/                   # Persistent Delta Lake tables (Medallion architecture)
├── etl/                    # ETL Python code (transformations, API clients)
├── notebooks/              # Jupyter notebooks for analysis
├── tests/                  # Pytest unit and integration tests
├── .env                    # Environment variables for Docker Compose
├── docker-compose.yml      # Defines all services and infrastructure
└── README.md               # This file
```

---

## Delta Lake Warehouse Structure

The data is organized into a Medallion architecture with three layers:

```
data/
└── warehouse/
    ├── bronze.db/
    │   ├── divvy_bikes
    │   ├── ghibli_films
    │   ├── ghibli_locations
    │   ├── ghibli_people
    │   ├── ghibli_species
    │   ├── ghibli_vehicles
    │   ├── olist_customers_dataset
    │   ├── olist_geolocation_dataset
    │   ├── olist_order_items_dataset
    │   ├── olist_order_payments_dataset
    │   ├── olist_order_reviews_dataset
    │   ├── olist_orders_dataset
    │   ├── olist_products_dataset
    │   ├── olist_sellers_dataset
    │   └── olist_product_category_name_translation
    │
    ├── silver.db/
    │   ├── brewery
    │   ├── brewery_daily
    │   ├── divvy_bikes_status
    │   ├── divvy_station_information
    │   ├── divvy_station_status
    │   ├── divvy_system_pricing_plan
    │   ├── divvy_vehicle_types
    │   ├── ghibli_films
    │   ├── ghibli_locations
    │   ├── ghibli_people
    │   ├── ghibli_species
    │   ├── ghibli_vehicles
    │   ├── olist_customers_dataset
    │   ├── olist_geolocation_dataset
    │   ├── olist_order_items_dataset
    │   ├── olist_order_payments_dataset
    │   ├── olist_order_reviews_dataset
    │   ├── olist_orders_dataset
    │   ├── olist_products_dataset
    │   ├── olist_sellers_dataset
    │   └── olist_product_category_name_translation
    │
    └── gold.db/
        ├── countries_brewery_type_num
        ├── divvy_bikes_status
        ├── divvy_station_information
        ├── divvy_station_status
        ├── divvy_system_pricing_plan
        ├── divvy_vehicle_types
        ├── olist_delivery_time_table
        └── olist_sellers_performance
```

---

## Getting Started

### Prerequisites
- Docker Desktop
- Git

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/AirflowDeltaIgnite.git
cd AirflowDeltaIgnite

# 2. Build and start the services
docker compose up --build -d

# 3. Access the UIs
- Airflow: http://localhost:8082 (login: airflow/airflow)
- Spark Master: http://localhost:8080
- Jupyter Lab: http://localhost:8888
```

### Run a DAG
1.  Go to the Airflow UI → **DAGs**.
2.  Find a DAG (e.g., `divvy_bikes_ingestion`) and un-pause it.
3.  Trigger the DAG and monitor its execution.
4.  Check the `./data/warehouse` directory to see the created Delta tables.

---

## Adding a New Project

1.  **Duplicate Structure**: Copy an existing project's folders in `dags/` and `etl/`.
2.  **Customize Logic**: Modify the DAG and ETL scripts in the new folders for your project's needs.
3.  **Restart Airflow**: The new DAG will appear automatically. If you make changes to files outside the `dags` folder, restart the relevant service.
    `docker compose restart airflow-scheduler airflow-webserver`

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
