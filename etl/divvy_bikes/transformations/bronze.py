from common.utils import Now
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from common.DeltaSpark import DeltaSpark
from pyspark.sql import DataFrame as SparkDataFrame
from divvy_bikes.utils.Paths import raw_bronze, spark_path
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

        print(f"┌{'─'*118}┐")
        print(r"____/\\\\\\\\\\\\\______/\\\\\\\\\___________/\\\\\_______/\\\\\_____/\\\__/\\\\\\\\\\\\\\\__/\\\\\\\\\\\\\\\___________")
        print(r"____\/\\\/////////\\\__/\\\///////\\\_______/\\\///\\\____\/\\\\\\___\/\\\_\////////////\\\__\/\\\///////////___________")
        print(r"_____\/\\\_______\/\\\_\/\\\_____\/\\\_____/\\\/__\///\\\__\/\\\/\\\__\/\\\___________/\\\/___\/\\\_____________________")
        print(r"______\/\\\\\\\\\\\\\\__\/\\\\\\\\\\\/_____/\\\______\//\\\_\/\\\//\\\_\/\\\_________/\\\/_____\/\\\\\\\\\\\____________")
        print(r"_______\/\\\/////////\\\_\/\\\//////\\\____\/\\\_______\/\\\_\/\\\\//\\\\/\\\_______/\\\/_______\/\\\///////____________")
        print(r"________\/\\\_______\/\\\_\/\\\____\//\\\___\//\\\______/\\\__\/\\\_\//\\\/\\\_____/\\\/_________\/\\\__________________")
        print(r"_________\/\\\_______\/\\\_\/\\\_____\//\\\___\///\\\__/\\\____\/\\\__\//\\\\\\___/\\\/___________\/\\\_________________")
        print(r"__________\/\\\\\\\\\\\\\/__\/\\\______\//\\\____\///\\\\\/_____\/\\\___\//\\\\\__/\\\\\\\\\\\\\\\_\/\\\\\\\\\\\\\\\____")
        print(r"___________\/////////////____\///________\///_______\/////_______\///_____\/////__\///////////////__\///////////////____")
        print(f"│{' ' * 32} ,---.               |                              {' ' * 34}│")
        print(f"│{' ' * 32} `---.,---.,---.,---.|__/                     __o   {' ' * 34}│")
        print(f"│{' ' * 32}     ||   |,---||    |  \                   _ \\<_   {' ' * 34}│")
        print(f"│{' ' * 32} `---'|---'`---^`    `   ` 3.5.7    ...... (_)/(_)  {' ' * 34}│")
        print(f"└{'─'*118}┘")


    def load_raw_data_to_bronze(self, divvy_path):


        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {raw_bronze + divvy_path}.json -> bronze.divvy_bikes',
                         start=True)

        if not DeltaTable.isDeltaTable(self.spark, f'data/warehouse/bronze.db/divvy_bikes'):
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

            (df.write.format('delta')
                     .mode('append').option("mergeSchema", "true")
                     .partitionBy('type')
                     .save(f'{spark_path}/data/warehouse/bronze.db/divvy_bikes'))

        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {divvy_path}.json -> bronze.divvy_{divvy_path} | OK',
                         end=True)

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA | OK', start=True, end=True)