from common.utils import Now, delta_logos
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from common.DeltaSpark import DeltaSpark
from olist.utils.paths import spark_path
from pyspark.sql import DataFrame as SparkDataFrame
from olist.utils.logo import olist_logo, spark_logo
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, TimestampType,
                               IntegerType, DecimalType)


class Silver(Now):
    _SHOW_LOG: bool = True

    _SCHEMAS = {
        'olist_customers_dataset': StructType(
            [StructField('customer_id',              StringType(), False),
             StructField('customer_unique_id',       StringType(), False),
             StructField('customer_zip_code_prefix', StringType(), True),
             StructField('customer_city',            StringType(), True),
             StructField('customer_state',           StringType(), True),
             ]),
        'olist_geolocation_dataset': StructType(
            [StructField("geolocation_zip_code_prefix", StringType(), False),
             StructField("geolocation_lat",             DoubleType(), True),
             StructField("geolocation_lng",             DoubleType(), True),
             StructField("geolocation_city",            StringType(), True),
             StructField("geolocation_state",           StringType(), True),
             ]),
        'olist_order_items_dataset': StructType(
            [StructField("order_id",                         StringType(), False),
             StructField("customer_id",                      StringType(), False),
             StructField("order_status",                     StringType(),  True),
             StructField("order_purchase_timestamp",      TimestampType(),  True),
             StructField("order_approved_at",             TimestampType(),  True),
             StructField("order_delivered_carrier_date",  TimestampType(),  True),
             StructField("order_delivered_customer_date", TimestampType(),  True),
             StructField("order_estimated_delivery_date", TimestampType(),  True),
             ]),
        'olist_order_payments_dataset': StructType(
            [StructField('order_id',                 StringType(),      False),
             StructField('payment_sequential',       IntegerType(),     True),
             StructField('payment_type',             StringType(),      True),
             StructField('payment_installments',     IntegerType(),     True),
             StructField('payment_value',            DecimalType(10,4), True),
             ]),
        'olist_order_reviews_dataset': StructType(
            [StructField("review_id",               StringType(),    False),
             StructField("order_id",                StringType(),    False),
             StructField("review_score",            IntegerType(),   True),
             StructField("review_comment_title",    StringType(),    True),
             StructField("review_comment_message",  StringType(),    True),
             StructField("review_creation_date",    TimestampType(), True),
             StructField("review_answer_timestamp", TimestampType(), True),
             ]),
        'olist_orders_dataset': StructType(
            [StructField("order_id",                         StringType(), False),
             StructField("customer_id",                      StringType(), False),
             StructField("order_status",                     StringType(),  True),
             StructField("order_purchase_timestamp",      TimestampType(),  True),
             StructField("order_approved_at",             TimestampType(),  True),
             StructField("order_delivered_carrier_date",  TimestampType(),  True),
             StructField("order_delivered_customer_date", TimestampType(),  True),
             StructField("order_estimated_delivery_date", TimestampType(),  True),
             ]),
        'olist_products_dataset': StructType(
            [StructField("product_id",                  StringType(), False),
             StructField("product_category_name",       StringType(),  True),
             StructField("product_name_lenght",        IntegerType(),  True),
             StructField("product_description_lenght", IntegerType(),  True),
             StructField("product_weight_g",           IntegerType(),  True),
             StructField("product_length_cm",          IntegerType(),  True),
             StructField("product_height_cm",          IntegerType(),  True),
             StructField("product_width_cm",           IntegerType(),  True),
             ]),
        'olist_sellers_dataset': StructType(
            [StructField('seller_id',              StringType(), False),
             StructField('seller_zip_code_prefix', StringType(), True),
             StructField('seller_city',            StringType(), True),
             StructField('seller_state',           StringType(), True),
             ]),
        'product_category_name_translation': StructType(
            [StructField('product_category_name',         StringType(), False),
             StructField('product_category_name_english', StringType(), False),
             ])
    }

    def __init__(self, spark: SparkSession=None):
        if not spark:
            self.spark: SparkSession = DeltaSpark().initialize()
        else:
            self.spark: SparkSession = spark

        delta_logos('silver')
        olist_logo()
        spark_logo()


    def run_silver(self, delta_file_name:str=None) -> bool:
        # -- STEP 1 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='CHECKING SCHEMA', start=True)
        if delta_file_name not in self._SCHEMAS.keys():
            raise ValueError(f"{delta_file_name} not in {self._SCHEMAS.keys()}")
        self.log_message(show=self._SHOW_LOG, message='CHECKING SCHEMA | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # -- STEP 2 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'LOADING SILVER | {delta_file_name}', start=True)
        df: SparkDataFrame = self.spark.read.format('delta')\
                            .load(f"{spark_path}/data/warehouse/bronze.db/{delta_file_name}")
        self.log_message(show=self._SHOW_LOG, message=f'LOADING SILVER | {delta_file_name} | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # -- STEP 3 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'GETTING THE MOST RECENT DATA | {delta_file_name}', start=True)
        df: SparkDataFrame = df.withColumn('dat_ref_carga_formated',
                                           to_date('dat_ref_carga', 'yyyyMMddHH'))
        # --------------------------------------------------------------------------------------------------------------

        # -- STEP 4 ----------------------------------------------------------------------------------------------------
        dat_ref: str = df.orderBy('dat_ref_carga_formated', ascending=False)\
                         .select('dat_ref_carga').distinct()\
                         .collect()[0][0]
        self.log_message(show=self._SHOW_LOG, message=dat_ref, sep='.')
        # --------------------------------------------------------------------------------------------------------------

        # -- STEP 5 ----------------------------------------------------------------------------------------------------
        df: SparkDataFrame = df.filter(col('dat_ref_carga') == dat_ref)\
                               .drop('dat_ref_carga_formated', 'dat_ref_carga')
        self.log_message(show=self._SHOW_LOG,
                         message=f'GETTING THE MOST RECENT DATA | {delta_file_name} | OK',
                         end=True)
        # --------------------------------------------------------------------------------------------------------------

        # -- STEP 6 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'SAVING TO SILVER | {delta_file_name}', start=True)
        df.write.format('delta').mode('overwrite').save(f"{spark_path}/data/warehouse/silver.db/{delta_file_name}")
        self.log_message(show=self._SHOW_LOG, message=f'SAVING TO SILVER | {delta_file_name} | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        return True