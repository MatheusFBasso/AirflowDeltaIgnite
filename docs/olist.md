# Olist Pipeline Documentation

## Overview

The Olist pipeline processes data from the [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), which contains information about 100,000 orders made between 2016 and 2018. The pipeline ingests and transforms this data using the Medallion architecture, storing the results in Delta Tables.

- **Focus**: E-commerce analytics, including order fulfillment, customer satisfaction, and seller performance.
- **DAGs**: `dags/olist/` orchestrates the entire ETL process.
- **ETL Code**: `etl/olist/` handles data ingestion and PySpark/Delta transformations.
- **Data Location**: `/opt/spark/data/warehouse/` (bronze/silver/gold in `warehouse.db`).
- **Spark Version**: 4.0.1
- **Python Version**: 3.12

---

## Pipeline Flow

1.  **Ingestion**: Fetches the Olist datasets and lands them in the raw layer.
2.  **Bronze**: Partitions the raw data into structured Delta Tables.
3.  **Silver**: Cleans, joins, and enriches the data, applying data quality checks.
4.  **Gold**: Aggregates the data to create business-level insights, such as seller performance and delivery time metrics.

---

## Extract (Ingestion)

**DAG**: `OlistDag.py` – Schedules the ingestion of the Olist datasets into the raw layer.

**ETL**: `etl/olist/apis/olist_api.py` – Ingests the datasets from local CSV files.

**Key Steps**:
-   The Olist datasets are read from CSV files.
-   The raw data is saved to `/opt/spark/data/warehouse/raw_data/olist/`.

---

## Bronze Layer

**ETL**: `etl/olist/transformations/bronze.py` – Partitions the raw data into Delta Tables.

**Key Steps**:
-   Read from raw: `spark.read.format("csv").load("/opt/spark/data/warehouse/raw_data/olist/*.csv")`.
-   Write to `/opt/spark/data/warehouse/bronze.db/` with one table per dataset (e.g., `olist_orders_dataset`).

---

## Silver Layer

**ETL**: `etl/olist/transformations/silver.py` – Cleans, joins, and validates the data.

**Key Steps**:
-   The bronze tables are read and joined (e.g., orders and payments).
-   Data types are cast to the correct format, and data is deduplicated.
-   Data quality checks are performed to ensure data integrity.
-   The cleaned and joined data is written to `/opt/spark/data/warehouse/silver.db/`.

**Example Code Snippet** (from `silver.py`):
```python
from etl.common.utils import get_spark_session, check_data_quality

spark = get_spark_session("OlistSilver")
orders_df = spark.read.format("delta").load("/opt/spark/data/warehouse/bronze.db/olist_orders_dataset")
payments_df = spark.read.format("delta").load("/opt/spark/data/warehouse/bronze.db/olist_order_payments_dataset")

joined_df = orders_df.join(payments_df, "order_id")

quality_report = check_data_quality(joined_df, ["order_id"])
if quality_report['nulls']['order_id'] > 0:
    raise ValueError("Quality check failed: Nulls found in order_id column")

joined_df.write.format("delta").mode("overwrite").save("/opt/spark/data/warehouse/silver.db/olist_orders_and_payments")
```

---

## Gold Layer

**ETL**: `etl/olist/transformations/gold.py` – Aggregates data for business insights.

**Key Steps**:
-   The silver data is aggregated to create meaningful KPIs, such as seller performance and delivery time analysis.
-   The aggregated data is written to `/opt/spark/data/warehouse/gold.db/`.

**Example Code Snippet** (from `gold.py`):
```python
from pyspark.sql.functions import col, avg, count
from etl.common.utils import get_spark_session

spark = get_spark_session("OlistGold")
silver_df = spark.read.format("delta").load("/opt/spark/data/warehouse/silver.db/olist_orders_and_payments")

seller_performance_df = silver_df.groupBy("seller_id").agg(
    count("order_id").alias("total_orders"),
    avg("payment_value").alias("average_order_value")
)

seller_performance_df.write.format("delta").mode("overwrite").save("/opt/spark/data/warehouse/gold.db/seller_performance")
```
