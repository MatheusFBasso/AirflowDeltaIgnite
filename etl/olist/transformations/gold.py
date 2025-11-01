from common.utils import Now, delta_logos
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, datediff
from common.DeltaSpark import DeltaSpark
from olist.utils.paths import spark_path
from pyspark.sql import DataFrame as SparkDataFrame
from olist.utils.logo import olist_logo, spark_logo
from pyspark.sql.types import DecimalType, DateType, IntegerType
from datetime import datetime

class Gold(Now):
    _SHOW_LOG = True
    _SHOW = True

    def __init__(self, spark: SparkSession):

        if not spark:
            self.spark: SparkSession = DeltaSpark().initialize()
        else:
            self.spark: SparkSession = spark

        delta_logos('gold')
        olist_logo()
        spark_logo()


    def safe_load_silver_delta_tables(self, table_name:str, view_name:str=None, return_df: bool=True):
        self.log_message(show=self._SHOW_LOG, message=f'LOADING SILVER | {table_name}', start=True)
        df: SparkDataFrame = self.spark.read\
                                 .format('delta')\
                                 .load(f"{spark_path}/data/warehouse/silver.db/{table_name}")

        if view_name is None and return_df is True:
            self.log_message(show=self._SHOW_LOG, message=f'returning spark dataframe', sep='.')

        elif view_name is not None and return_df is False:
            self.log_message(show=self._SHOW_LOG, message=f'creating view {view_name}', sep='.')
            df.createOrReplaceTempView(view_name)
            self.log_message(show=self._SHOW_LOG, message=f'creating view {view_name} | OK', sep='.')
            self.log_message(show=self._SHOW_LOG, message=f'LOADING SILVER | {table_name}', end=True)
            return None

        else:
            self.log_message(show=self._SHOW_LOG, message=f'returning spark dataframe', sep='.')
            self.log_message(show=self._SHOW_LOG, message=f'creating view {view_name}', sep='.')
            df.createOrReplaceTempView(view_name)
            self.log_message(show=self._SHOW_LOG, message=f'creating view {view_name} | OK', sep='.')

        self.log_message(show=self._SHOW_LOG, message=f'LOADING SILVER | {table_name}', end=True)
        return df


    def create_orders_diff(self, orders_df: SparkDataFrame) -> SparkDataFrame:
        self.log_message(show=self._SHOW_LOG, message=f'CREATING ORDERS DIFF', start=True)
        df = (orders_df.
             withColumn('delivery_diff_promissed',
                        datediff(
                            col('order_delivered_customer_date'),
                            col('order_estimated_delivery_date')
                                 )).
             select('order_id', 'customer_id', 'delivery_diff_promissed').
             where(col('delivery_diff_promissed').isNotNull())
             )
        self.log_message(show=self._SHOW_LOG, message=f'CREATING ORDERS DIFF | OK', end=True)
        return df


    def delivery_time_table(self):
        # -- Step 1 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='LOADING ORDERS', start=True)
        df_orders: SparkDataFrame = self.spark.read.format('delta')\
                                        .load(f'{spark_path}/data/warehouse/silver.db/olist_orders_dataset')
        self.log_message(show=self._SHOW_LOG, message='LOADING ORDERS | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # -- Step 2 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='GETTING ORDERS DIFF', start=True)
        df_orders_diff: SparkDataFrame = self.create_orders_diff(df_orders)

        df_orders = df_orders.join(df_orders_diff, (df_orders.order_id == df_orders_diff.order_id) & (
                    df_orders.customer_id == df_orders_diff.customer_id), how='inner').where(
            col('order_status') == 'delivered')

        del df_orders_diff
        self.log_message(show=self._SHOW_LOG, message='GETTING ORDERS DIFF | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # -- Step 3 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='APPLYING BUSINESS RULES', start=True)
        df_orders = (df_orders.withColumn('analise_entrega',
                                          when((col('delivery_diff_promissed') <= 0) & (
                                                      col('delivery_diff_promissed') >= -5), 'B | BOM').
                                          when((col('delivery_diff_promissed') <= -6) & (
                                                      col('delivery_diff_promissed') >= -12), 'O | OTIMO').
                                          when(col('delivery_diff_promissed') < -12, 'E | EXCELENTE').
                                          when((col('delivery_diff_promissed') >= 1) & (
                                                      col('delivery_diff_promissed') <= 3), 'T | TOLERAVEL').
                                          when((col('delivery_diff_promissed') >= 4) & (
                                                      col('delivery_diff_promissed') <= 7), 'R | RUIM').
                                          when((col('delivery_diff_promissed') >= 8) & (
                                                      col('delivery_diff_promissed') <= 14), 'G | GRAVE').
                                          when((col('delivery_diff_promissed') >= 15) & (
                                                      col('delivery_diff_promissed') <= 28), 'P | PESSIMO').
                                          when(col('delivery_diff_promissed') > 28, 'U | URGENTE')
                                          ))

        df_orders_sigla = df_orders.groupBy('analise_entrega').count().withColumn('porcentagem', (
                    col('count') / df_orders.count() * 100).cast(DecimalType(5, 2)))
        df_orders_sigla = (df_orders_sigla.withColumnRenamed('analise_entrega', 'sigla').
                           withColumnRenamed('count', 'ordens').
                           withColumn('date_ref_carga', lit(datetime.now()).cast(DateType())).
                           withColumn('ordens', col('ordens').cast(IntegerType())))

        del df_orders
        self.log_message(show=self._SHOW_LOG, message='APPLYING BUSINESS RULES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # -- Step 3 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING INTO gold.olist_delivery_time_table', start=True)
        df_orders_sigla.write.format('delta').mode('overwrite')\
                       .save(f'{spark_path}/data/warehouse/gold.db/olist_delivery_time_table')
        self.log_message(show=self._SHOW_LOG, message='SAVING INTO gold.olist_delivery_time_table | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        print('')

        return True


    def sellers_performance(self):
        # -- Step 1 ----------------------------------------------------------------------------------------------------
        self.safe_load_silver_delta_tables(table_name='olist_orders_dataset', view_name='orders')
        self.safe_load_silver_delta_tables(table_name='olist_order_items_dataset', view_name='olist_order_items')
        self.safe_load_silver_delta_tables(table_name='olist_order_payments_dataset', view_name='olist_payments')
        self.safe_load_silver_delta_tables(table_name='olist_sellers_dataset', view_name='olist_sellers')
        # --------------------------------------------------------------------------------------------------------------

        # -- Step 2 ----------------------------------------------------------------------------------------------------
        self.create_orders_diff(self.spark.table('orders')).createTempView('orders_datediff')
        # --------------------------------------------------------------------------------------------------------------

        # -- Step 3 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'APLLYING BUSINESS RULES', start=True)
        df = self.spark.sql("""
        WITH

        sigla AS (
             SELECT order_id, 
                  CASE WHEN delivery_diff_promissed <=  0 AND delivery_diff_promissed >=  -5 THEN 'B | BOM'
                       WHEN delivery_diff_promissed <= -6 AND delivery_diff_promissed >= -12 THEN 'O | OTIMO'
                       WHEN delivery_diff_promissed < -12                                    THEN 'E | EXCELENTE'
                       WHEN delivery_diff_promissed >=  1 AND delivery_diff_promissed <=  3  THEN 'T | TOLERAVEL'
                       WHEN delivery_diff_promissed >=  4 AND delivery_diff_promissed <=  7  THEN 'R | RUIM'
                       WHEN delivery_diff_promissed >=  8 AND delivery_diff_promissed <= 14  THEN 'G | GRAVE'
                       WHEN delivery_diff_promissed >= 15 AND delivery_diff_promissed <= 28  THEN 'P | PESSIMO'
                       WHEN delivery_diff_promissed >  28                                    THEN 'U | URGENTE'
                       END AS sigla 
             FROM orders_datediff
        ),
        
        sigla_agrupada AS (
             SELECT DISTINCT oi.seller_id, s.sigla, 
                  COUNT(s.sigla) OVER(PARTITION BY oi.seller_id, s.sigla ORDER BY oi.seller_id) AS n_pedidos,
                  ROUND(n_pedidos /
                  COUNT(seller_id) OVER(PARTITION BY oi.seller_id ORDER BY oi.seller_id) *100, 2) AS proporcional
             FROM olist_order_items AS oi
                  INNER JOIN sigla AS s
                       ON oi.order_id = s.order_id
             ORDER BY proporcional DESC
        ),
        
        sigla_final AS (
          SELECT seller_id, COLLECT_LIST(CONCAT(sigla, '(', proporcional, '%)')) AS resumo, SUM(n_pedidos) AS n_pedidos
          FROM sigla_agrupada
          GROUP BY seller_id
        ),
        
        orders AS (
          SELECT oi.order_id, oi.seller_id
          FROM olist_order_items as oi
        ),
        
        sellers_total AS (
          SELECT o.seller_id, SUM(p.payment_value) AS total
          FROM orders AS o
            INNER JOIN olist_payments AS p
              ON o.order_id = p.order_id
          WHERE p.payment_value IS NOT NULL
          GROUP BY o.seller_id
        ),
        
        sellers AS (
          SELECT DISTINCT seller_id, seller_city, seller_state
          FROM olist_sellers
        )
        
        SELECT s.seller_id                                           AS id_vendedor, 
               s.seller_city                                         AS cidade, 
               s.seller_state                                        AS estado, 
               total                                                 AS total, 
               CAST(n_pedidos AS INTEGER)                            AS pedidos, 
               COALESCE(CAST(sf.resumo AS STRING), 'Nao localizado') AS resumo_entregas,
               CAST(NOW() AS DATE)                                   AS date_ref_carga
        FROM sellers AS s
          INNER JOIN sellers_total AS st
            ON s.seller_id = st.seller_id
          LEFT JOIN sigla_final AS sf
            ON s.seller_id = sf.seller_id
        ORDER BY total DESC, sf.n_pedidos DESC
        """)

        self.log_message(show=self._SHOW_LOG, message=f'APLLYING BUSINESS RULES | OK', end = True)
        # --------------------------------------------------------------------------------------------------------------

        # -- Step 4 ----------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message=f'Saving gold.sellers_performance', start=True)
        df.write.format('delta').mode('overwrite')\
            .partitionBy('date_ref_carga')\
            .save(f'{spark_path}/data/warehouse/gold.db/sellers_performance')
        self.log_message(show=self._SHOW_LOG, message=f'Saving gold.sellers_performance | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        print('')
        return True