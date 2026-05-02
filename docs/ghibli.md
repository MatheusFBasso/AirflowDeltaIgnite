# Ghibli Pipeline Documentation

## Overview

The Ghibli pipeline processes data from the [Studio Ghibli API](https://ghibliapi.herokuapp.com), a public dataset of films, characters, and locations from Studio Ghibli's animated movies. The pipeline ingests data from the API, transforms it through the Medallion architecture, and stores the results in Delta Tables.

- **Focus**: Film and character analysis from the Studio Ghibli universe.
- **DAGs**: `dags/ghibli/` orchestrates the entire ETL process.
- **ETL Code**: `etl/ghibli/` handles API fetching and PySpark/Delta transformations.
- **Data Location**: `/opt/spark/data/warehouse/` (bronze/silver/gold in `warehouse.db`).
- **Spark Version**: 4.0.1
- **Python Version**: 3.12

---

## Pipeline Flow

1.  **Ingestion**: Fetches data from the Studio Ghibli API and lands it in the raw layer.
2.  **Bronze**: Partitions the raw data into structured Delta Tables.
3.  **Silver**: Cleans and enriches the data, applying data quality checks.
4.  **Gold**: Aggregates the data to create business-level insights.

---

## Extract (Ingestion)

**DAG**: `GhibliDag.py` – Schedules the API calls to ingest data into the raw layer.

**ETL**: `etl/ghibli/apis/ghibli_api.py` – Fetches film and character data.

**Key Steps**:
-   API calls are made to the Studio Ghibli API to get lists of films and people.
-   The raw JSON responses are saved to `/opt/spark/data/warehouse/raw_data/ghibli/`.

**Example Code Snippet** (from `ghibli_api.py`):
```python
import requests
from pyspark.sql import SparkSession

def fetch_ghibli_films(spark: SparkSession):
    response = requests.get("https://ghibliapi.herokuapp.com/films")
    df = spark.createDataFrame(response.json())
    df.write.format("json").mode("overwrite").save("/opt/spark/data/warehouse/raw_data/ghibli/films")
```

---

## Bronze Layer

**ETL**: `etl/ghibli/transformations/bronze.py` – Partitions the raw data into Delta Tables.

**Key Steps**:
-   Read from raw: `spark.read.format("json").load("/opt/spark/data/warehouse/raw_data/ghibli/films")`.
-   Write to `/opt/spark/data/warehouse/bronze.db/ghibli_films` with Delta configurations.

---

## Silver Layer

**ETL**: `etl/ghibli/transformations/silver.py` – Cleans and validates the data.

**Key Steps**:
-   The bronze data is read and deduplicated.
-   Data types are cast to the correct format.
-   Data quality checks are performed to ensure data integrity.
-   The cleaned data is written to `/opt/spark/data/warehouse/silver.db/ghibli_films`.

**Example Code Snippet** (from `silver.py`):
```python
from etl.common.utils import get_spark_session, check_data_quality

spark = get_spark_session("GhibliSilver")
bronze_df = spark.read.format("delta").load("/opt/spark/data/warehouse/bronze.db/ghibli_films")
silver_df = bronze_df.dropDuplicates(["id"])
quality_report = check_data_quality(silver_df, ["id"])
if quality_report['nulls']['id'] > 0:
    raise ValueError("Quality check failed: Nulls found in id column")
silver_df.write.format("delta").mode("overwrite").save("/opt/spark/data/warehouse/silver.db/ghibli_films")
```

---

## Gold Layer

There is currently no Gold layer for the Ghibli pipeline. The Silver data is ready for consumption and analysis.
