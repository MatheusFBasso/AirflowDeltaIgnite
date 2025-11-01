import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from olist.utils.classes_call import OlistClassesCall
from olist.utils.paths import raw_bronze as BRONZE_PATH_RAW_DATA
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Function to generate bash command for cleaning (used for both pre and post)
def get_bash_command(path_name: str, is_post: bool = False) -> str:
    prefix = "postclean" if is_post else "bkp"
    bash_command = """
            # Retrieve parameters
            path="{{ params.base_path }}""" + path_name + """"
            file_pattern="{{ params.file_pattern }}"
            n_days="{{ params.n_days }}"

            # Create path if it doesn't exist
            mkdir -p "${path}"

            # Generate timestamp
            timestamp=$(date +%Y_%m_%d_%H_%M_%S)

            # Create bkp subfolder if it doesn't exist
            bkp_dir="${path}bkp"
            mkdir -p "${bkp_dir}"

            # Debug: List files in path
            echo "Files in ${path}:"
            ls -l "${path}" || echo "No files found or path error"

            # Count matching files
            moved_count=$(find "${path}" -maxdepth 1 -type f -name "${file_pattern}" | wc -l)

            # Move all matching files to bkp, prepending timestamp and prefix to filename
            find "${path}" -maxdepth 1 -type f -name "${file_pattern}" -exec sh -c '
                base=$(basename "$0")
                mv "$0" "${1}/${2}_""" + prefix + """_${base}"
            ' {} "${bkp_dir}" "${timestamp}" \;

            # Log moved files
            if [ ${moved_count} -gt 0 ]; then
                echo "${moved_count} files moved to ${bkp_dir} successfully."
            else
                echo "No files matching ${file_pattern} found to move."
            fi

            # Delete files in bkp older than n_days
            deleted_count=$(find "${bkp_dir}" -type f -mtime +${n_days} | wc -l)
            find "${bkp_dir}" -type f -mtime +${n_days} -delete

            # Log deleted files
            if [ ${deleted_count} -gt 0 ]; then
                echo "${deleted_count} files older than ${n_days} days in ${bkp_dir} deleted successfully."
            else
                echo "No files older than ${n_days} days found in ${bkp_dir}."
            fi
            """
    return bash_command

# DAG definition
with DAG(
    dag_id='OlistDag',
    schedule='0 9 * * *',
    max_active_tasks=2,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    description='Divvy Bikes DAG for multi-hop architecture in Spark Delta Tables.',
    tags=['DivvyBikes', 'DeltaTable', 'Spark', 'Information', 'Pricing'],
    params={
        'base_path': f'/opt/airflow/{BRONZE_PATH_RAW_DATA}',
        'file_pattern': '*.csv',
        'n_days': '0',
    },
    dagrun_timeout=timedelta(minutes=5.0),
) as dag:
    # Dynamically create tasks for each extraction type

    pre_clean = BashOperator(
        task_id=f'pre_clean_olist_raw_data',
        bash_command=get_bash_command(path_name='', is_post=False)
    )

    pos_clean = BashOperator(
        task_id=f'pos_clean_olist_raw_data',
        bash_command=get_bash_command(path_name='', is_post=True)
    )

    with TaskGroup(group_id='raw_to_bronze') as tgb:


        extract_task = PythonOperator(
            task_id=f'extract_olist_raw_data',
            python_callable=lambda **kwargs: OlistClassesCall('get_raw_data',
                owner_slug= 'olistbr',
                dataset_slug='brazilian-ecommerce',
                extract_dir= BRONZE_PATH_RAW_DATA),
            provide_context=True,
            max_active_tis_per_dag=1,
        )

        bronze_task = SparkSubmitOperator(
            task_id=f'raw_to_bronze',
            application='/opt/airflow/etl/olist/transformations/bronze_to_delta.py',
            conn_id='spark_conn',
            deploy_mode='client',
            executor_cores=8,
            total_executor_cores=8,
            driver_memory='10G',
            executor_memory='10G',
            conf={
                'spark.sql.debug.maxToStringFields': '1000',
            },
            name=f'olist-bronze-data',
            verbose=False,
            application_args=['olist_bronze'],
            dag=dag
        )

        extract_task >> bronze_task

    with TaskGroup(group_id=f'bronze_to_silver') as tgs:
        for table in ['olist_customers_dataset', 'olist_geolocation_dataset', 'olist_order_items_dataset',
                      'olist_order_payments_dataset', 'olist_order_reviews_dataset', 'olist_orders_dataset',
                      'olist_products_dataset', 'olist_sellers_dataset', 'product_category_name_translation']:
            silver_task = SparkSubmitOperator(
                task_id=f'silver_{table}',
                application='/opt/airflow/etl/olist/transformations/silver_to_delta.py',
                conn_id='spark_conn',
                deploy_mode='client',
                executor_cores=5,
                total_executor_cores=5,
                driver_memory='8G',
                executor_memory='8G',
                conf={
                    'spark.sql.debug.maxToStringFields': '1000',
                },
                name=f'divvy-silver-{table}',
                verbose=False,
                application_args=[table],
                dag=dag
            )

    with TaskGroup(group_id=f'silver_to_gold') as tgg:
        for table in ['delivery_time_table', 'sellers_performance']:
            silver_task = SparkSubmitOperator(
                task_id=f'gold_{table}',
                application='/opt/airflow/etl/olist/transformations/gold_to_delta.py',
                conn_id='spark_conn',
                deploy_mode='client',
                executor_cores=5,
                total_executor_cores=5,
                driver_memory='8G',
                executor_memory='8G',
                conf={
                    'spark.sql.debug.maxToStringFields': '1000',
                },
                name=f'divvy-gold-{table}',
                verbose=False,
                application_args=[table],
                dag=dag
            )

    tgb >> pos_clean
    pre_clean >> tgb >> tgs >> tgg