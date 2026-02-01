import pendulum
from airflow import DAG
from datetime import timedelta
from common.utils import get_bash_command
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from brewery.utils.classes_call import BreweryCall
from brewery.utils.paths import raw_bronze as BRONZE_PATH_RAW_DATA
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# List of extraction types with their specific paths
extractions = [
    {
        'id': 'brewery',
        'python_callable': lambda **kwargs: BreweryCall('get_raw_data', **kwargs)
    },
]


# DAG definition
with DAG(
    dag_id='BreweryDag',
    schedule='*/20 * * * *',
    max_active_tasks=2,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    description='Brewery DAG for multi-hop architecture in Spark Delta Tables.',
    tags=['Brewery', 'DeltaTable', 'Spark'],
    params={
        'base_path': f'/opt/airflow/{BRONZE_PATH_RAW_DATA}',
        'file_pattern': 'PART_*.json',
        'n_days': '1',
    },
    dagrun_timeout=timedelta(minutes=5.0),
) as dag:
    # Dynamically create tasks for each extraction type
    for extraction in extractions:
        with TaskGroup(group_id=extraction['id']) as tg:

            pre_clean = BashOperator(
                task_id=f'pre_clean_{extraction["id"]}',
                bash_command=get_bash_command(path_name='', is_post=False)
            )

            extract_task = PythonOperator(
                task_id=f'extract_{extraction["id"]}',
                python_callable=extraction['python_callable'],
                max_active_tis_per_dag=1,
                retries=2,
                retry_delay=timedelta(minutes=1),
            )

            silver_task = SparkSubmitOperator(
                task_id=f'silver_{extraction["id"]}',
                application='/opt/airflow/etl/brewery/transformations/silver_to_delta.py',
                conn_id='spark_conn',
                deploy_mode='client',
                executor_cores=5,
                total_executor_cores=5,
                driver_memory='8G',
                executor_memory='8G',
                conf={
                    'spark.sql.debug.maxToStringFields': '1000',
                },
                name=f'divvy-silver-{extraction["id"]}',
                verbose=False,
                application_args=['silver_' + extraction['id']],
                dag=dag
            )

            gold_task = SparkSubmitOperator(
                task_id=f'gold_{extraction["id"]}',
                application='/opt/airflow/etl/brewery/transformations/gold_to_delta.py',
                conn_id='spark_conn',
                deploy_mode='client',
                executor_cores=10,
                total_executor_cores=10,
                driver_memory='8G',
                executor_memory='8G',
                conf={
                    'spark.sql.debug.maxToStringFields': '1000',
                },
                name=f'divvy-silver-{extraction["id"]}',
                verbose=False,
                application_args=['silver_' + extraction['id']],
                dag=dag
            )

            pos_clean = BashOperator(
                task_id=f'pos_clean_{extraction["id"]}',
                bash_command=get_bash_command(path_name='', is_post=True)
            )

            # Chain tasks: pre_clean >> extract >> bronze >> post_clean
            pre_clean >> extract_task >> silver_task >> gold_task >> pos_clean