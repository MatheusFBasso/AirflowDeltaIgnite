from common.utils import Now, delta_logos, safe_save_to_delta
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json
from common.DeltaSpark import DeltaSpark
from pyspark.sql import DataFrame as SparkDataFrame
from ghibli.utils.paths import raw_bronze, spark_path
from ghibli.utils.logo import ghibli_logo, spark_logo
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
        ghibli_logo()
        spark_logo()


    def load_raw_data_to_bronze(self, ghibli_path:str):


        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {raw_bronze}/{ghibli_path}/*.json -> bronze.ghibli',
                         start=True)

        if not DeltaTable.isDeltaTable(self.spark, f'{spark_path}/data/warehouse/bronze.db/ghibli_{ghibli_path}'):
            print('Creating Bronze Table')

            (self.spark.read
             .option("multiLine", True)
             .json(raw_bronze + '/' + ghibli_path + '/')
             .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
             .select('type', 'data', 'last_updated', 'ttl', 'version', 'last_updated_ts')
             .write.format('delta')
             .partitionBy('type')
             .mode('append').option("mergeSchema", "true")
             .save(f'{spark_path}/data/warehouse/bronze.db/ghibli_{ghibli_path}')
             )

        else:
            print('Using Bronze Table')
            df: SparkDataFrame = (
                self.spark.read
                       .option("multiLine", True)
                       .json(raw_bronze + '/' + ghibli_path + '/')
                       .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
                       .select('type', 'data', 'last_updated', 'ttl', 'version', 'last_updated_ts'))

            safe_save_to_delta(df=df,
                               delta_layer='bronze',
                               table_name=f'ghibli_{ghibli_path}',
                               mode='append',
                               partition_by='type',
                               spark_path=spark_path,
                               merge_schema=True)

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA | OK', start=True, end=True)