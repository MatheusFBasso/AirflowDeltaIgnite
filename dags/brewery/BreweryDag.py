import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from brewery.utils.ClassesCall import BreweryCall
from brewery.utils.Paths import raw_bronze as BRONZE_PATH_RAW_DATA
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# List of extraction types with their specific paths
extractions = [
    {
        'id': 'brewery',
        'python_callable': lambda **kwargs: BreweryCall('get_raw_data', **kwargs)
    },
]

# Function to generate bash command for cleaning (used for both pre and post)
def get_bash_command(is_post: bool = False) -> str:
    prefix = "postclean" if is_post else "bkp"
    bash_command = """
            # Retrieve parameters
            path="{{ params.base_path }}/"
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
        'n_days': '7',
    },
    dagrun_timeout=timedelta(minutes=5.0),
) as dag:
    # Dynamically create tasks for each extraction type
    for extraction in extractions:
        with TaskGroup(group_id=extraction['id']) as tg:

            pre_clean = BashOperator(
                task_id=f'pre_clean_{extraction["id"]}',
                bash_command=get_bash_command(is_post=False)
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
                bash_command=get_bash_command(is_post=True)
            )

            # Chain tasks: pre_clean >> extract >> bronze >> post_clean
            pre_clean >> extract_task >> silver_task >> pos_clean