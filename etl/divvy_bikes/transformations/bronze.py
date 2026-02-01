from common.utils import Now, delta_logos, safe_save_to_delta
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from common.DeltaSpark import DeltaSpark
from pyspark.sql import DataFrame as SparkDataFrame
from divvy_bikes.utils.paths import raw_bronze, spark_path
from divvy_bikes.utils.logo import divvy_logo, spark_logo
from pyspark.sql.types import StructType, StructField, TimestampType, LongType, StringType


class Bronze(Now):

    _SHOW_LOG: bool = True

    BRONZE_SCHEMA = StructType([
            StructField('type', StringType(), False),
            StructField('data', StringType(), False),
            StructField('last_updated', LongType(), False),
            StructField('ttl', LongType(), False),
            StructField('version', StringType(), True),
            StructField('last_updated_ts', TimestampType(), True),
        ])

    def __init__(self, spark: SparkSession=None):
        if not spark:
            self.spark: SparkSession = DeltaSpark().initialize()
        else:
            self.spark: SparkSession = spark

        delta_logos('bronze')
        divvy_logo()
        spark_logo()


    def load_raw_data_to_bronze(self, divvy_path):


        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {raw_bronze + divvy_path}.json -> bronze.divvy_bikes',
                         start=True)

        if not DeltaTable.isDeltaTable(self.spark, f'{spark_path}/data/warehouse/bronze.db/divvy_bikes'):
            print('Creating Bronze Table')

            (self.spark.read.format('json').load(raw_bronze + '/' + divvy_path + '/')
             .withColumn('type', lit(divvy_path))
             .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
             .select('type', 'data', 'last_updated', 'ttl', 'version', 'last_updated_ts')
             .write.format('delta')
             .partitionBy('type')
             .mode('append').option("mergeSchema", "true")
             .save(f'{spark_path}/data/warehouse/bronze.db/divvy_bikes'))

        else:
            print('Using Bronze Table')
            df: SparkDataFrame = (
                self.spark.read.format('json').load(raw_bronze + '/' + divvy_path + '/')
                       .withColumn('type', lit(divvy_path))
                       .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
                       .select('type', 'data', 'last_updated', 'ttl', 'version', 'last_updated_ts'))

            safe_save_to_delta(df=df,
                               delta_layer='bronze',
                               table_name='divvy_bikes',
                               mode='append',
                               partition_by='type',
                               spark_path=spark_path,
                               merge_schema=True)
            (df.write.format('delta')
                     .mode('append').option("mergeSchema", "true")
                     .partitionBy('type')
                     .save(f'{spark_path}/data/warehouse/bronze.db/divvy_bikes'))

        # self.log_message(show=self._SHOW_LOG,
        #                  message=f'LOADING DATA | {divvy_path}.json -> bronze.divvy_{divvy_path} | OK',
        #                  end=True)

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA | OK', start=True, end=True)