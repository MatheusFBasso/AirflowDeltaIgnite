# Divvy Bikes Pipeline Documentation

## Overview

The Divvy Bikes pipeline processes data from the [Divvy Bike Share](https://divvybikes.com/system-data) system in Chicago. The pipeline ingests real-time station and bike status data, transforms it through the Medallion architecture, and stores the results in Delta Tables.

- **Focus**: Real-time bike share system monitoring and analysis.
- **DAGs**: `dags/divvy_bikes/` orchestrates the entire ETL process.
- **ETL Code**: `etl/divvy_bikes/` handles API fetching and PySpark/Delta transformations.
- **Data Location**: `/opt/spark/data/warehouse/` (bronze/silver/gold in `warehouse.db`).
- **Spark Version**: 4.0.1
- **Python Version**: 3.12

---

## Pipeline Flow

1.  **Ingestion**: Fetches data from the Divvy Bikes API and lands it in the raw layer.
2.  **Bronze**: Partitions the raw data into structured Delta Tables.
3.  **Silver**: Cleans and enriches the data, applying data quality checks.
4.  **Gold**: Aggregates the data to create business-level insights.

---

## Extract (Ingestion)

**DAGs**: `DivvyBikesDagStatus.py`, `DivvyBikesInfoPricing.py` – Schedule the API calls to ingest data into the raw layer.

**ETL**: `etl/divvy_bikes/apis/divvy_api.py` – Fetches station status, station information, and pricing plan data.

**Key Steps**:
-   API calls are made to the Divvy Bikes GBFS feeds.
-   The raw JSON responses are saved to `/opt/spark/data/warehouse/raw_data/divvy_bikes/`.

**Example Code Snippet** (from `divvy_api.py`):
```python
import requests
from pyspark.sql import SparkSession

def fetch_divvy_station_status(spark: SparkSession):
    response = requests.get("https://gbfs.divvybikes.com/gbfs/en/station_status.json")
    df = spark.createDataFrame(response.json()['data']['stations'])
    df.write.format("json").mode("overwrite").save("/opt/spark/data/warehouse/raw_data/divvy_bikes/station_status")
```

---

## Bronze Layer

**ETL**: `etl/divvy_bikes/transformations/bronze.py` – Partitions the raw data into Delta Tables.

**Key Steps**:
-   Read from raw: `spark.read.format("json").load("/opt/spark/data/warehouse/raw_data/divvy_bikes/station_status")`.
-   Write to `/opt/spark/data/warehouse/bronze.db/divvy_station_status` with Delta configurations.

---

## Silver Layer

**ETL**: `etl/divvy_bikes/transformations/silver.py` – Cleans and validates the data.

**Key Steps**:
-   The bronze data is read and deduplicated.
-   Data types are cast to the correct format.
-   Data quality checks are performed to ensure data integrity.
-   The cleaned data is written to `/opt/spark/data/warehouse/silver.db/divvy_station_status`.

**Example Code Snippet** (from `silver.py`):
```python
from etl.common.utils import get_spark_session, check_data_quality

spark = get_spark_session("DivvySilver")
bronze_df = spark.read.format("delta").load("/opt/spark/data/warehouse/bronze.db/divvy_station_status")
silver_df = bronze_df.dropDuplicates(["station_id"])
quality_report = check_data_quality(silver_df, ["station_id"])
if quality_report['nulls']['station_id'] > 0:
    raise ValueError("Quality check failed: Nulls found in station_id column")
silver_df.write.format("delta").mode("overwrite").save("/opt/spark/data/warehouse/silver.db/divvy_station_status")
```

---

## Gold Layer

**ETL**: `etl/divvy_bikes/transformations/gold.py` – Aggregates data for business insights.

**Key Steps**:
-   The silver data is aggregated to create meaningful KPIs, such as the number of available bikes per station.
-   The aggregated data is written to `/opt/spark/data/warehouse/gold.db/station_availability`.

**Example Code Snippet** (from `gold.py`):
```python
from pyspark.sql.functions import col
from etl.common.utils import get_spark_session

spark = get_spark_session("DivvyGold")
silver_df = spark.read.format("delta").load("/opt/spark/data/warehouse/silver.db/divvy_station_status")
gold_df = silver_df.select("station_id", "num_bikes_available", "num_docks_available")
gold_df.write.format("delta").mode("overwrite").save("/opt/spark/data/warehouse/gold.db/station_availability")
```
