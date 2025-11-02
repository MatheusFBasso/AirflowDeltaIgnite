<img width="1250" height="1210" alt="delta_ignite_olistdrawio" src="https://github.com/user-attachments/assets/3f27e631-e2bb-47eb-a090-2c2264618778" />

# Kaggle Dataset link:
## [Olist BR - Brazilian Ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

# Airflow Structure

## 1. - Pre Clean Files: 

Purpose
The `get_bash_command` function generates a parameterized bash command for the Airflow `BashOperator`. It automates file lifecycle management by:

- Moving files matching a pattern (e.g., `*.csv`) from a source directory to a backup subfolder (`bkp/`).
- Appending a timestamp and prefix (`bkp_` or `postclean_`) to filenames.
- Deleting backup files older than `n_days` (e.g., 2 days).

This promotes clean, reusable, and maintainable DAGs across projects (Divvy Bikes, Brewery, etc.) by centralizing logic in `etl/common/utils.py`.

---

Function Signature

  ````python
def get_bash_command(path_name: str, is_post: bool = False) -> str:
  ````

Parameter | Type | Description |
----------| ---- | ----------- |
`path_name` | `str`  | Subpath appended to `{{ params.base_path }}` (e.g., `divvy_bikes/free_bike_status/`). |
`is_post`   | `bool` | If `True`, uses `postclean_` prefix and `bkp/` folder. If `False`, uses `bkp_` prefix.

---

Returns: A fully formatted bash command string with Airflow templating.

## **Behavior Flow**

### 1 - **Banner Output** Displays a stylized ASCII banner for visual clarity in Airflow logs.
### 2 - **Parameter Injection** Constructs full path:
  ````bash
bashpath="{{ params.base_path }}divvy_bikes/free_bike_status/"
  ````
### 3 - Directory Setup
  - Ensures `${path}` and `${path}/bkp` exist (`mkdir -p`).

### 4 - Debug Logging
  - Lists files in `${path}` for traceability.

### 5 - File Movement
  - Finds files: `find ... -name "*.csv"`
  - Moves to `${bkp_dir}` with format:
  ````text
text{timestamp}_{prefix}_{original_filename}
  ````
  Example: 2025_10_26_19_30_15_bkp_data.csv


### 6 - Accurate Logging
  - Counts files before moving (`moved_count`) → correct success/failure message.

### 7 - Old File Cleanup
  - Counts files older than `n_days` (`deleted_count`) → accurate deletion log.
  - Deletes using `-mtime +${n_days}` (modification time).

---

## Example Usage in DAG
  ````python
from airflow.operators.bash import BashOperator
from etl.common.utils import get_bash_command

move_csv_files = BashOperator(
    task_id='cleanup_csv_files',
    bash_command=get_bash_command(
        path_name='divvy_bikes/trips/',
        is_post=False  # Use bkp_ prefix
    ),
    params={
        'base_path': '/opt/spark/data/raw_data/',
        'file_pattern': '*.csv',
        'n_days': '2'
    },
    dag=dag
)
````

Output in Ariflow Logs:
  ````text
Files in /opt/spark/data/raw_data/divvy_bikes/trips/:
-rw-r--r-- 1 user user 1024 Oct 26 19:00 trip_001.csv
3 files moved to /opt/spark/data/raw_data/divvy_bikes/trips/bkp successfully.
1 files older than 2 days in .../bkp deleted successfully.
````
---
## Key Improvements

| Feature             | Benefit                                                      |
| --------------------| --------------------------------------------------------     |
| Centralized Logic   | One function → consistent behavior across DAGs.              |
Templated Parameters  | Dynamic paths/patterns via Airflow params.                   |
Accurate Counting     | `moved_count`/`deleted_count` before action → truthful logs. |
Prefix Toggle         | `is_post=True` for post-processing cleanup (postclean_).     |
Robust Error Handling | Continues on empty dirs; logs clear status.                  |
