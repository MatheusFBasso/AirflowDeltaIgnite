from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame as SparkDataFrame


class Now:
    """
    A utility class for handling current time in a specific timezone and logging messages
    with timestamps and customizable formatting.

    This class provides methods to retrieve the current time as a string or datetime object
    and to log messages with optional start/end separators and padding.

    Attributes:
        show (bool): Default flag to control whether log messages are displayed.
    """

    def __init__(self, show: bool = True):
        """
        Initializes the Now instance.

        Args:
            show (bool, optional): Default visibility for log messages. Defaults to True.
        """
        self.show = show

    @staticmethod
    def now(timezone: str = 'America/Sao_Paulo') -> str:
        """
        Returns the current time as a formatted string in the specified timezone.

        Args:
            timezone (str, optional): Timezone name (e.g., 'America/Sao_Paulo').
                Defaults to 'America/Sao_Paulo'.

        Returns:
            str: Formatted current time (YYYY-MM-DD HH:MM:SS).

        Raises:
            zoneinfo.ZoneInfoNotFoundError: If the timezone is invalid.
        """
        return datetime.now(ZoneInfo(timezone)).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def now_datetime(timezone: str = 'America/Sao_Paulo') -> datetime:
        """
        Returns the current time as a datetime object in the specified timezone.

        Args:
            timezone (str, optional): Timezone name (e.g., 'America/Sao_Paulo').
                Defaults to 'America/Sao_Paulo'.

        Returns:
            datetime: Current datetime object in the specified timezone.

        Raises:
            zoneinfo.ZoneInfoNotFoundError: If the timezone is invalid.
        """
        return datetime.now(ZoneInfo(timezone))

    def log_message(self,
                    message: str,
                    start: bool = False,
                    end: bool = False,
                    sep: str = '-',
                    line_length: int = 120,
                    show: bool | None = None) -> None:
        """
        Logs a message with a timestamp, optional start/end lines, and padding.

        The message is printed in the format: "TIMESTAMP | MESSAGE | [padding]".
        Padding fills the line to `line_length` using `sep`. If the content exceeds
        `line_length`, no padding is added.

        Args:
            message (str): The message to log.
            start (bool, optional): If True, prints a full separator line before the message.
                Defaults to False.
            end (bool, optional): If True, prints a full separator line after the message.
                Defaults to False.
            sep (str, optional): Separator character(s) for padding and lines. Defaults to '-'.
            line_length (int, optional): Target length for the log line. Defaults to 120.
            show (bool | None, optional): If True, display the log; if False, suppress.
                If None, uses self.show. Defaults to None.

        Returns:
            None
        """
        if show is None:
            show = self.show

        if not show:
            return

        timestamp = self.now()
        sep_len = len(sep)
        extra_len = len(timestamp) + len(message) + 5  # For " | " and " |"

        if start:
            print(sep * line_length)

        if extra_len >= line_length:
            # No padding if content is too long
            print(f"{timestamp} | {message} |")
            if end:
                print(sep * line_length)
            return

        filler = ""
        if sep_len > 0:
            remaining = line_length - extra_len
            length_fill_line = remaining // sep_len
            total_filled = length_fill_line * sep_len
            complement = sep[:remaining - total_filled] if remaining > total_filled else ""
            filler = (sep * length_fill_line) + complement

        print(f"{timestamp} | {message} |{filler}")

        if end:
            print(sep * line_length)


def delta_logos(delta_layer: str) -> None:
    delta_layer: str = delta_layer.lower().strip()

    if delta_layer not in ['bronze', 'silver', 'gold']:
        raise TypeError('type must be either "bronze", "silver" or "gold"')

    if delta_layer == 'bronze':
        print(
            r"____/\\\\\\\\\\\\\______/\\\\\\\\\___________/\\\\\_______/\\\\\_____/\\\__/\\\\\\\\\\\\\\\__/\\\\\\\\\\\\\\\___________")
        print(
            r"____\/\\\/////////\\\__/\\\///////\\\_______/\\\///\\\____\/\\\\\\___\/\\\_\////////////\\\__\/\\\///////////___________")
        print(
            r"_____\/\\\_______\/\\\_\/\\\_____\/\\\_____/\\\/__\///\\\__\/\\\/\\\__\/\\\___________/\\\/___\/\\\_____________________")
        print(
            r"______\/\\\\\\\\\\\\\\__\/\\\\\\\\\\\/_____/\\\______\//\\\_\/\\\//\\\_\/\\\_________/\\\/_____\/\\\\\\\\\\\____________")
        print(
            r"_______\/\\\/////////\\\_\/\\\//////\\\____\/\\\_______\/\\\_\/\\\\//\\\\/\\\_______/\\\/_______\/\\\///////____________")
        print(
            r"________\/\\\_______\/\\\_\/\\\____\//\\\___\//\\\______/\\\__\/\\\_\//\\\/\\\_____/\\\/_________\/\\\__________________")
        print(
            r"_________\/\\\_______\/\\\_\/\\\_____\//\\\___\///\\\__/\\\____\/\\\__\//\\\\\\___/\\\/___________\/\\\_________________")
        print(
            r"__________\/\\\\\\\\\\\\\/__\/\\\______\//\\\____\///\\\\\/_____\/\\\___\//\\\\\__/\\\\\\\\\\\\\\\_\/\\\\\\\\\\\\\\\____")
        print(
            r"___________\/////////////____\///________\///_______\/////_______\///_____\/////__\///////////////__\///////////////____")
    if delta_layer == 'silver':
        print(
            r"________/\\\\\\\\\\\____/\\\\\\\\\\\__/\\\______________/\\\________/\\\__/\\\\\\\\\\\\\\\____/\\\\\\\\\________________")
        print(
            r"_______/\\\/////////\\\_\/////\\\///__\/\\\_____________\/\\\_______\/\\\_\/\\\///////////___/\\\///////\\\_____________")
        print(
            r"_______\//\\\______\///______\/\\\_____\/\\\_____________\//\\\______/\\\__\/\\\_____________\/\\\_____\/\\\____________")
        print(
            r"_________\////\\\_____________\/\\\_____\/\\\______________\//\\\____/\\\___\/\\\\\\\\\\\_____\/\\\\\\\\\\\/____________")
        print(
            r"_____________\////\\\__________\/\\\_____\/\\\_______________\//\\\__/\\\____\/\\\///////______\/\\\//////\\\___________")
        print(
            r"_________________\////\\\_______\/\\\_____\/\\\________________\//\\\/\\\_____\/\\\_____________\/\\\____\//\\\_________")
        print(
            r"___________/\\\______\//\\\______\/\\\_____\/\\\_________________\//\\\\\______\/\\\_____________\/\\\_____\//\\\_______")
        print(
            r"___________\///\\\\\\\\\\\/____/\\\\\\\\\\\_\/\\\\\\\\\\\\\\\______\//\\\_______\/\\\\\\\\\\\\\\\_\/\\\______\//\\\_____")
        print(
            r"______________\///////////_____\///////////__\///////////////________\///________\///////////////__\///________\///_____")
    if delta_layer == 'gold':
        print(
            '_' * 19 + r"_____/\\\\\\\\\\\\_______/\\\\\_______/\\\______________/\\\\\\\\\\\\____________" + '_' * 20)
        print(
            '_' * 19 + r"____/\\\//////////______/\\\///\\\____\/\\\_____________\/\\\////////\\\_________" + '_' * 20)
        print(
            '_' * 19 + r"____/\\\_______________/\\\/__\///\\\__\/\\\_____________\/\\\______\//\\\_______" + '_' * 20)
        print(
            '_' * 19 + r"____\/\\\____/\\\\\\\__/\\\______\//\\\_\/\\\_____________\/\\\_______\/\\\______" + '_' * 20)
        print(
            '_' * 19 + r"_____\/\\\___\/////\\\_\/\\\_______\/\\\_\/\\\_____________\/\\\_______\/\\\_____" + '_' * 20)
        print(
            '_' * 19 + r"______\/\\\_______\/\\\_\//\\\______/\\\__\/\\\_____________\/\\\_______\/\\\____" + '_' * 20)
        print(
            '_' * 19 + r"_______\/\\\_______\/\\\__\///\\\__/\\\____\/\\\_____________\/\\\_______/\\\____" + '_' * 20)
        print(
            '_' * 19 + r"________\//\\\\\\\\\\\\/_____\///\\\\\/_____\/\\\\\\\\\\\\\\\_\/\\\\\\\\\\\\/____" + '_' * 20)
        print(
            '_' * 19 + r"__________\////////////_________\/////_______\///////////////__\////////////_____" + '_' * 20)


def safe_save_to_delta(
        df: SparkDataFrame,
        delta_layer: str,
        table_name: str,
        mode: str = 'overwrite',
        partition_by: Optional[str] = None,
        spark_path: str = '/opt/airflow',
        merge_schema: bool = False,
        log: bool = True
) -> bool:
    """
    Safely saves a Spark DataFrame to a Delta table in the specified layer.

    This function validates inputs, logs parameters and progress if enabled, displays a short sample of the data for verification,
    and writes the DataFrame to a Delta table path based on the provided layer (bronze, silver, or gold). It supports
    overwrite or append modes, optional partitioning, and schema merging.

    Args:
        df (SparkDataFrame): The Spark DataFrame to save.
        delta_layer (str): The Delta layer to save to ('bronze', 'silver', or 'gold').
        table_name (str): The name of the table to save.
        mode (str, optional): Save mode ('overwrite' or 'append'). Defaults to 'overwrite'.
        partition_by (Optional[str], optional): Column to partition by. Defaults to None.
        spark_path (str, optional): Base path for Spark storage. Defaults to '/opt/airflow'.
        merge_schema (bool, optional): Whether to merge schema on write. Defaults to False.
        log (bool, optional): Whether to log progress messages and parameters. Defaults to True.

    Returns:
        bool: True if the save was successful.

    Raises:
        ValueError: If delta_layer or mode is invalid.
        Exception: If the write operation fails (e.g., Spark errors).
    """
    now = Now()

    mode = mode.lower().strip()
    delta_layer = delta_layer.lower().strip()
    table_name = table_name.lower().strip()

    now.log_message(show=log, message=f'Saving {delta_layer}.{table_name}', start=True)

    if log:
        params_info = f"""
        Parameters:
            ├── delta_layer={delta_layer}
            ├── table_name={table_name}
            ├── mode={mode}, 
            ├── partition_by={partition_by}
            ├── spark_path={spark_path}
            └── merge_schema={merge_schema}"""
        now.log_message(show=log, message=params_info, sep='.')

    delta_paths = {
        'gold': '/data/warehouse/gold.db/',
        'silver': '/data/warehouse/silver.db/',
        'bronze': '/data/warehouse/bronze.db/',
    }

    if delta_layer not in delta_paths:
        raise ValueError(f"Delta layer {delta_layer} is not supported.")

    if mode not in ['overwrite', 'append']:
        raise ValueError(f"Mode {mode} is not supported.")

    if log:
        now.log_message(show=log, message=' ── checking table', sep='.')
        df.show(1, truncate=50)
        df.printSchema()

    save_path = spark_path + delta_paths[delta_layer] + table_name

    writer = df.write.format('delta').mode(mode)

    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    try:
        writer.save(save_path)
    except Exception as e:
        now.log_message(show=log, message=f'Saving {delta_layer}.{table_name} | FAILED: {str(e)}', end=True)
        raise  # Re-raise to propagate the error, but log first.

    now.log_message(show=log, message=f'Saving {delta_layer}.{table_name} | OK', end=True)

    return True


def get_bash_command(path_name: str, is_post: bool = False) -> str:
    prefix = "postclean" if is_post else "bkp"
    return """# Retrieve parameters
                echo "░█▀▀░▀█▀░█░░░█▀▀░░░█▄█░█▀█░█▀█░█▀█░█▀▀░█▀▀░█▀▄░"
                echo "░█▀▀░░█░░█░░░█▀▀░░░█░█░█▀█░█░█░█▀█░█░█░█▀▀░█▀▄░"
                echo "░▀░░░▀▀▀░▀▀▀░▀▀▀░░░▀░▀░▀░▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀░▀░"

                path="{{ params.base_path }}""" + path_name + """"
                file_pattern="{{ params.file_pattern }}"
                n_days="{{ params.n_days }}"

                # Create path if it doesn't exist
                mkdir -p "${path}"

                # Generate timestamp (YYYY_MM_DD_HH_MM_SS)
                timestamp=$(date +%Y_%m_%d_%H_%M_%S)

                # Create bkp subfolder if it doesn't exist
                bkp_dir="${path}/bkp"
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

                # ---------------------------------------------------------
                # UPDATE: Delete files in bkp based on FILENAME Date
                # ---------------------------------------------------------

                # 1. Calculate cutoff date string (YYYY_MM_DD)
                cutoff_date=$(date -d "-${n_days} days" +%Y_%m_%d)

                # 2. Find and Filter
                # We capture the list of files to delete in a variable first
                files_to_delete=$(find "${bkp_dir}/" -maxdepth 1 -type f | awk -v limit="${cutoff_date}" -F/ '{
                    filename = $NF
                    # Extract the first 10 chars (YYYY_MM_DD)
                    file_date = substr(filename, 1, 10)

                    # Check if file matches date pattern AND is alphabetically lower (older) than limit
                    if (file_date ~ /^[0-9]{4}_[0-9]{2}_[0-9]{2}$/ && file_date < limit) {
                        print $0
                    }
                }')

                # 3. Delete and Log
                if [ -n "$files_to_delete" ]; then
                    # Count the files in the variable
                    deleted_count=$(echo "$files_to_delete" | wc -l)

                    # Delete the files listed in the variable
                    echo "$files_to_delete" | xargs rm -f

                    echo "${deleted_count} files older than ${n_days} days (cutoff: ${cutoff_date}) deleted successfully from ${bkp_dir}/."
                else
                    echo "No files older than ${n_days} days (cutoff: ${cutoff_date}) found in ${bkp_dir}/."
                fi
            """