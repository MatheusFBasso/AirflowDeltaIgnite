from glob import glob
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from common.DeltaSpark import DeltaSpark
from common.utils import Now, delta_logos
from olist.utils.logo import olist_logo, spark_logo
from olist.utils.paths import raw_bronze, spark_path


class Bronze(Now):

    DATE_FORMAT:str = "%Y-%m-%d"
    TODAY:str = datetime.now().strftime(DATE_FORMAT)
    _SHOW_LOG:bool = True

    def __init__(self, spark: SparkSession) -> None:
        delta_logos('bronze')
        olist_logo()
        spark_logo()

        if not spark:
            self.spark: SparkSession = DeltaSpark().initialize()
        else:
            self.spark: SparkSession = spark


    @staticmethod
    def find_files(path: str) -> list:
        return glob(path)


    def process_files(self, path: str=None) -> bool:

        path:str = path if path else raw_bronze

        self.log_message(show=self._SHOW_LOG, message=f'LOOKING FOR RAW_DATA AT: {path}', start=True)
        files_list:list = self.find_files(path + '*.csv')
        if len(files_list) > 0:
            self.log_message(show=self._SHOW_LOG, message=f'LOOKING FOR RAW_DATA AT: {path} | OK', end=True)
        else:
            self.log_message(show=self._SHOW_LOG, message=f'LOOKING FOR RAW_DATA AT: {path} | NOK | No files located', start=True)
            return True

        self.log_message(show=self._SHOW_LOG, message=f'STARTING TO LOAD CSV FROM {path}', start=True)
        for _file in files_list:
            _table_name:str = Path(_file).stem
            self.log_message(show=self._SHOW_LOG,
                             message=f'Loading "{_table_name}.csv" -> Bronze: "{_table_name}"',
                             sep='.')
            self.spark.read .option("header", "true")\
                            .option("inferSchema", "false")\
                            .format('csv')\
                            .load(_file)\
                            .withColumn('dat_ref_carga', lit(self.TODAY))\
                            .write.format('delta')\
                            .mode('overwrite') \
                            .option("mergeSchema", "true")\
                            .partitionBy("dat_ref_carga")\
                            .save(f'{spark_path}/data/warehouse/bronze.db/{_table_name}')
            self.log_message(show=self._SHOW_LOG,
                             message=f'Loading "{_table_name}.csv" -> Bronze: "{_table_name}" | OK',
                             sep='.')
        self.log_message(show=self._SHOW_LOG, message=f'STARTING TO LOAD CSV FROM {path}', end=True)
        return True