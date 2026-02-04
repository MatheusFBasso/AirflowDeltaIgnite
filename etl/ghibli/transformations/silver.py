from pprint import pprint
from datetime import datetime
from typing import List, Union
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from common.DeltaSpark import DeltaSpark
from ghibli.utils.paths import spark_path
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame as SparkDataFrame
from ghibli.utils.logo import ghibli_logo, spark_logo
from pyspark.sql.functions import explode, col, max, current_timestamp
from common.utils import Now, delta_logos, safe_save_to_delta, delta_upsert


class Silver(Now):
    _SHOW_LOG: bool = True
    _START_TIME: datetime = Now().now_datetime()
    _CONFIGS: dict = {
        "films": {
            "table_name": "ghibli_films",
            "bronze_path": f'{spark_path}/data/warehouse/bronze.db/ghibli_films',
            "silver_path": f'{spark_path}/data/warehouse/silver.db/ghibli_films',
            "match_keys": "id",
            "explode_col": "data",
            "except_cols": ['type', 'ttl', 'version'],
            "flatten_cols": ["id", "title", "original_title", "original_title_romanised",
                         "image", "movie_banner", "description", "director", "producer",
                         "release_date", "running_time", "rt_score", "people", "species",
                         "locations", "vehicles", "url"]
        },

        "locations": {
            "table_name": "ghibli_locations",
            "bronze_path": f'{spark_path}/data/warehouse/bronze.db/ghibli_locations',
            "silver_path": f'{spark_path}/data/warehouse/silver.db/ghibli_locations',
            "match_keys": "id",
            "explode_col": "data",
            "except_cols": ['type', 'ttl', 'version'],
            "flatten_cols": ["id", "name", "climate", "terrain","surface_water", "residents", "films", "url"]
        },

        "people": {
            "table_name": "ghibli_people",
            "bronze_path": f'{spark_path}/data/warehouse/bronze.db/ghibli_people',
            "silver_path": f'{spark_path}/data/warehouse/silver.db/ghibli_people',
            "match_keys": "id",
            "explode_col": "data",
            "except_cols": ['type', 'ttl', 'version'],
            "flatten_cols": ["id", "name", "gender", "age", "eye_color", "hair_color", "films", "species", "url"]
        },

        "species": {
            "table_name": "ghibli_species",
            "bronze_path": f'{spark_path}/data/warehouse/bronze.db/ghibli_species',
            "silver_path": f'{spark_path}/data/warehouse/silver.db/ghibli_species',
            "match_keys": "id",
            "explode_col": "data",
            "except_cols": ['type', 'ttl', 'version'],
            "flatten_cols": ["id", "name", "classification", "eye_colors", "hair_colors", "people", "films", "url"]
        },

        "vehicles": {
            "table_name": "ghibli_vehicles",
            "bronze_path": f'{spark_path}/data/warehouse/bronze.db/ghibli_vehicles',
            "silver_path": f'{spark_path}/data/warehouse/silver.db/ghibli_vehicles',
            "match_keys": "id",
            "explode_col": "data",
            "except_cols": ['type', 'ttl', 'version'],
            "flatten_cols": ["id", "name", "description", "vehicle_class", "length", "pilot", "films", "url"]
        }
    }

    def __init__(self, spark: SparkSession=None):
        if not spark:
            self.spark: SparkSession = DeltaSpark().initialize()
        else:
            self.spark: SparkSession = spark

        delta_logos('silver')
        ghibli_logo()
        spark_logo()
        print(self._START_TIME)

    def _get_bronze_table(self, bronze_path: str, table_name:str) -> SparkDataFrame:
        """
        Dynamically loads the bronze table based on the provided name.
        Assumes a naming convention (e.g., 'bronze_tablename').
        """
        try:
            # Example convention: source is always prefixed with 'bronze_'
            self.log_message(show=self._SHOW_LOG, message=f'LOADING GHIBLI bronze.{table_name} FROM {bronze_path}')
            df: SparkDataFrame = self.spark.read.format('delta').load(bronze_path)

            self.log_message(show=self._SHOW_LOG, message=f'Getting the latest data')

            max_date: str = df.select(max('last_updated_ts')).collect()[0][0]
            df: SparkDataFrame = df.filter(col('last_updated_ts') == max_date).distinct()

            self.log_message(show=self._SHOW_LOG, message=f'Getting the latest data | OK | {max_date}')
            self.log_message(show=self._SHOW_LOG, message=f'LOADING GHIBLI bronze.{table_name} FROM {bronze_path} | OK')

            return df
        except AnalysisException as e:
            self.log_message(show=self._SHOW_LOG,
                             message=f'LOADING GHIBLI bronze.{table_name} FROM {bronze_path} | FAILED | {e}')
            raise e

    @staticmethod
    def _step_1_standardize_columns(df: SparkDataFrame) -> SparkDataFrame:
        """Step 1: Clean column names (lower case, replace spaces)."""
        new_columns = [col(c).alias(c.lower().replace(" ", "_")) for c in df.columns]
        return df.select(*new_columns)


    def _step_2_dynamic_transform(self, df: SparkDataFrame, table_name: str) -> SparkDataFrame:
        """
        Applies transformations based on the 'Indexed' configuration.
        """
        self.log_message(show=self._SHOW_LOG, message=f'STEP 2: Dynamic transformation {table_name}')

        config = self._CONFIGS.get(table_name)

        self.log_message(show=self._SHOW_LOG, message=f'Setting configurations')
        pprint(config)

        # 1. Explode Logic (if configured)
        self.log_message(show=self._SHOW_LOG, message=f'Exploding and flattening column')
        explode_col = config.get("explode_col")
        if explode_col and explode_col in df.columns:
            print(f"Exploding column: {explode_col}")
            df = df.withColumn(explode_col, explode(col(explode_col)))

            # 2. Flatten/Select Logic (specific to the exploded column)
            # Assumption: The exploded column is now a Struct
            fields_to_extract = config.get("flatten_cols", [])
            renames = config.get("rename_map", {})

            if fields_to_extract:
                # Create expressions: col("items.price").alias("price")
                extract_exprs = []
                for field in fields_to_extract:
                    alias_name = renames.get(field, field)
                    extract_exprs.append(
                        col(f"{explode_col}.{field}").alias(alias_name)
                    )

                # Select all other columns + the extracted ones
                other_cols = [col(c) for c in df.columns if c != explode_col]
                df = df.select(*other_cols, *extract_exprs).distinct()

        self.log_message(show=self._SHOW_LOG, message=f'STEP 2: Dynamic transformation {table_name} | OK')
        return df.select([col for col in df.columns if col not in self._CONFIGS.get(table_name).get("except_cols")])

    @staticmethod
    def _step_3_filter_invalid_rows(df: SparkDataFrame) -> SparkDataFrame:
        """Step 4: Business logic filtering (removing null IDs)."""
        if "id" in df.columns:
            return df.filter(col("id").isNotNull()).distinct()
        return df

    @staticmethod
    def _step_4_add_audit_metadata(df: SparkDataFrame) -> SparkDataFrame:
        """Step 5: Add metadata columns (Run ID, Processing Time)."""
        if 'last_updated' in df.columns:
            return df.withColumn("date_ref_carga", col('last_updated').cast(TimestampType())) \
                     .withColumn("processed_at", current_timestamp())
        return df


    def _safe_save_table(self,
                           df: SparkDataFrame,
                           silver_path:str,
                           match_keys:Union[List[str], str],
                           table_name:str,
                           ):

        assert df.count() > 1, 'No Data'

        if not DeltaTable.isDeltaTable(self.spark, silver_path):
            self.log_message(show=self._SHOW_LOG, message='SAVING GHIBLI FILMS TABLE | FIRST EXECUTION', start=True)
            safe_save_to_delta(df=df,
                               delta_layer='silver',
                               table_name=table_name,
                               mode='append',
                               partition_by='date_ref_carga',
                               spark_path=spark_path,
                               merge_schema=True)
            self.log_message(show=self._SHOW_LOG, message='SAVING GHIBLI FILMS TABLE | FIRST EXECUTION | OK',
                             end=True)

        else:
            self.log_message(show=self._SHOW_LOG, message='UPSERT GHIBLI FILMS TABLE', start=True)
            delta_upsert(spark_session=self.spark,
                         match_keys=match_keys,
                         df=df,
                         delta_path=silver_path)

            self.log_message(show=self._SHOW_LOG, message='UPSERT GHIBLI FILMS TABLE | OK', end=True)


    def initialize(self, endpoint:str):
        self.log_message(show=self._SHOW_LOG, message=f'RUNNING SILVER STEP FOR {endpoint}', start=True)
        # --------------------------------------------------------------------------------------------------------------
        # Loading bronze
        df = self._get_bronze_table(bronze_path=self._CONFIGS.get(endpoint).get('bronze_path'),
                                    table_name=self._CONFIGS.get(endpoint).get('table_name'))
        #---------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'STEP 1: Standardize Columns {endpoint}')
        df = self._step_1_standardize_columns(df=df)
        self.log_message(show=self._SHOW_LOG, message=f'STEP 1: Standardize Columns {endpoint} | OK')
        # --------------------------------------------------------------------------------------------------------------
        # Explode and flatten the Dataframe
        df = self._step_2_dynamic_transform(df=df, table_name=endpoint)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'STEP 3: Filter Invalid Rows {endpoint}')
        df = self._step_3_filter_invalid_rows(df=df)
        self.log_message(show=self._SHOW_LOG, message=f'STEP 3: Filter Invalid Rows {endpoint} | OK')
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'STEP 4: Audit Metadata {endpoint}')
        df = self._step_4_add_audit_metadata(df=df)
        self.log_message(show=self._SHOW_LOG, message=f'STEP 4: Audit Metadata {endpoint} | OK')
        # --------------------------------------------------------------------------------------------------------------
        self._safe_save_table(
            df=df,
            silver_path=self._CONFIGS.get(endpoint).get('silver_path'),
            match_keys=self._CONFIGS.get(endpoint).get('match_keys'),
            table_name=self._CONFIGS.get(endpoint).get('table_name'),
        )
        # --------------------------------------------------------------------------------------------------------------
        self.spark.stop()
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'RUNNING SILVER STEP FOR {endpoint} | OK', end=True)
