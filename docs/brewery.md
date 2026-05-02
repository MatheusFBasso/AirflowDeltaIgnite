# Brewery Pipeline Documentation

## Overview

The Brewery pipeline processes data from the [Open Brewery DB](https://www.openbrewerydb.org/), a public dataset of breweries in the United States. The pipeline ingests data from the API, transforms it through the Medallion architecture (Bronze, Silver, and Gold), and stores the results in Delta Tables.

- **Focus**: Brewery location and attribute analysis.
- **DAGs**: `dags/brewery/` orchestrates the entire ETL process.
- **ETL Code**: `etl/brewery/` handles API fetching and PySpark/Delta transformations.
- **Data Location**: `/opt/spark/data/warehouse/` (bronze/silver/gold in `warehouse.db`).
- **Spark Version**: 4.0.1
- **Python Version**: 3.12

---

## Pipeline Flow

1.  **Ingestion**: Fetches data from the Open Brewery DB API and lands it in the raw layer.
2.  **Bronze**: Partitions the raw data into a structured Delta Table.
3.  **Silver**: Cleans and enriches the data, applying data quality checks.
4.  **Gold**: Aggregates the data to create business-level insights.

---

## Extract (Ingestion)

**DAG**: `BreweryDag.py` – Schedules the API calls to ingest data into the raw layer.

**ETL**: `etl/brewery/apis/brewery_api.py` – Fetches brewery data using `requests`.

**Key Steps**:
-   An API call is made to the Open Brewery DB to get a list of breweries.
-   The raw JSON response is saved to `/opt/spark/data/warehouse/raw_data/brewery/`.

**Example Code Snippet** (from `brewery_api.py`):
```python
import requests
from pyspark.sql import SparkSession

def fetch_brewery_data(spark: SparkSession):
    response = requests.get("https://api.openbrewerydb.org/breweries")
    df = spark.createDataFrame(response.json())
    df.write.format("json").mode("overwrite").save("/opt/spark/data/warehouse/raw_data/brewery/")
```

---

## Bronze Layer

**ETL**: The raw data is read and written to a partitioned Delta table in the bronze layer.

**Key Steps**:
-   Read from raw: `spark.read.format("json").load("/opt/spark/data/warehouse/raw_data/brewery/")`.
-   Write to `/opt/spark/data/warehouse/bronze.db/brewery` with Delta configurations.

---

## Silver Layer

**ETL**: `etl/brewery/transformations/silver.py` – Cleans and validates the data.

**Key Steps**:
-   The bronze data is read and deduplicated.
-   Data types are cast to the correct format.
-   Data quality checks are performed to ensure data integrity.
-   The cleaned data is written to `/opt/spark/data/warehouse/silver.db/brewery`.

**Example Code Snippet** (from `silver.py`):
```python
from etl.common.utils import get_spark_session, check_data_quality

spark = get_spark_session("BrewerySilver")
bronze_df = spark.read.format("delta").load("/opt/spark/data/warehouse/bronze.db/brewery")
silver_df = bronze_df.dropDuplicates(["id"])
quality_report = check_data_quality(silver_df, ["id"])
if quality_report['nulls']['id'] > 0:
    raise ValueError("Quality check failed: Nulls found in id column")
silver_df.write.format("delta").mode("overwrite").save("/opt/spark/data/warehouse/silver.db/brewery")
```

---

## Gold Layer

**ETL**: `etl/brewery/transformations/gold.py` – Aggregates data for business insights.

**Key Steps**:
-   The silver data is aggregated to create meaningful KPIs, such as the number of breweries per state.
-   The aggregated data is written to `/opt/spark/data/warehouse/gold.db/breweries_by_state`.

**Example Code Snippet** (from `gold.py`):
```python
from pyspark.sql.functions import col
from etl.common.utils import get_spark_session

spark = get_spark_session("BreweryGold")
silver_df = spark.read.format("delta").load("/opt/spark/data/warehouse/silver.db/brewery")
gold_df = silver_df.groupBy("state").count().withColumnRenamed("count", "brewery_count")
gold_df.write.format("delta").mode("overwrite").save("/opt/spark/data/warehouse/gold.db/breweries_by_state")
```
