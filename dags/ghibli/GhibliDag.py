import pendulum
from airflow import DAG
from datetime import timedelta
from common.utils import get_bash_command
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ghibli.utils.classes_call import GhibliCall
from ghibli.utils.paths import raw_bronze as BRONZE_PATH_RAW_DATA
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# List of extraction types with their specific paths
extractions = [
    {
        'id': 'films',
        'path_suffix': 'films',
        'python_callable': lambda **kwargs: GhibliCall('get_raw_data', **kwargs)
    },
    {
        'id': 'people',
        'path_suffix': 'people',
        'python_callable': lambda **kwargs: GhibliCall('get_raw_data', **kwargs)
    },
    {
        'id': 'locations',
        'path_suffix': 'locations',
        'python_callable': lambda **kwargs: GhibliCall('get_raw_data', **kwargs)
    },
    {
        'id': 'species',
        'path_suffix': 'species',
        'python_callable': lambda **kwargs: GhibliCall('get_raw_data', **kwargs)
    },
    {
        'id': 'vehicles',
        'path_suffix': 'vehicles',
        'python_callable': lambda **kwargs: GhibliCall('get_raw_data', **kwargs)
    },
]


# DAG definition
with DAG(
    dag_id='GhibliDag',
    schedule='*/20 * * * *',
    max_active_tasks=2,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    description='Ghibli DAG for multi-hop architecture in Spark Delta Tables.',
    tags=['Ghibli', 'DeltaTable', 'Spark'],
    params={
        'base_path': f'/opt/airflow/{BRONZE_PATH_RAW_DATA}',
        'file_pattern': 'extracted_at_*.json',
        'n_days': '7',
    },
    dagrun_timeout=timedelta(minutes=5.0),
) as dag:
    # Dynamically create tasks for each extraction type
    for extraction in extractions:
        with TaskGroup(group_id=extraction['id']) as tg:

            pre_clean = BashOperator(
                task_id=f'pre_clean_{extraction["id"]}',
                bash_command=get_bash_command(path_name=extraction['path_suffix'], is_post=False)
            )

            extract_task = PythonOperator(
                task_id=f'extract_{extraction["id"]}',
                python_callable=extraction['python_callable'],
                op_kwargs={'endpoint': extraction["id"]},
                max_active_tis_per_dag=1,
                retries=2,
                retry_delay=timedelta(minutes=1),
            )

            bronze_task = SparkSubmitOperator(
                task_id=f'bronze_{extraction["id"]}',
                application='/opt/airflow/etl/ghibli/transformations/bronze_to_delta.py',
                conn_id='spark_conn',
                deploy_mode='client',
                executor_cores=5,
                total_executor_cores=5,
                num_executors=1,
                driver_memory='16G',
                executor_memory='6G',
                retries=4,
                retry_delay=timedelta(seconds=5),
                conf={
                    'spark.sql.debug.maxToStringFields': '1000',
                },
                name=f'ghibli-bronze-{extraction["id"]}',
                verbose=False,
                application_args=[extraction['id']],
                dag=dag
            )

            silver_task = SparkSubmitOperator(
                task_id=f'silver_{extraction["id"]}',
                application='/opt/airflow/etl/ghibli/transformations/silver_to_delta.py',
                conn_id='spark_conn',
                deploy_mode='client',
                executor_cores=5,
                total_executor_cores=5,
                driver_memory='8G',
                executor_memory='8G',
                conf={
                    'spark.sql.debug.maxToStringFields': '1000',
                },
                name=f'ghibli-silver-{extraction["id"]}',
                verbose=False,
                application_args=[extraction["id"]],
                dag=dag
            )

            pre_clean >> extract_task >> bronze_task >> silver_task