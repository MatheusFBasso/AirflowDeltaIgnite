from pathlib import Path
from typing import Dict, List
from pyspark.sql import SparkSession
from .utils import Now


class DeltaSpark(Now):
    """
    Manages the SparkSession lifecycle with Delta Lake configurations
    and handles metadata recovery from the filesystem.
    """

    # Default Configuration
    DEFAULT_APP_NAME = 'PySparkDelta'
    DEFAULT_WAREHOUSE = '/opt/spark/data/warehouse'

    # Delta & Spark Configs
    _SPARK_CONFIGS = {
        "spark.eventLog.enabled": "false",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.submit.deployMode": "client"
    }

    _SPARK_CONFIGS_SET = {
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.databricks.delta.retryCommitAttempts": "10"
    }

    def __init__(self, app_name: str = DEFAULT_APP_NAME, warehouse_dir: str = DEFAULT_WAREHOUSE):
        """
        Args:
            app_name (str): Name of the Spark Application.
            warehouse_dir (str): System path to the Hive/Delta warehouse.
        """
        super().__init__()
        self._app_name = app_name
        self._warehouse_path = Path(warehouse_dir)
        self._show_logs = False

    def initialize(self, recover_metadata: bool = False) -> SparkSession:
        """
        Initializes the SparkSession and optionally recovers metadata.
        """
        self.log_message(show=self._show_logs, start=True, sep='=', message='STARTING SPARK')

        builder = SparkSession.builder.appName(self._app_name)

        # Apply configurations
        for key, value in self._SPARK_CONFIGS.items():
            builder.config(key, value)

        # Build spark session
        spark = builder.getOrCreate()

        # Set spark session configurations
        for key, value in self._SPARK_CONFIGS_SET.items():
            spark.conf.set(key, value)

        self._print_banner()
        self.log_message(show=self._show_logs, end=True, sep='=', message='SPARK | DELTA | INITIALIZED')

        if recover_metadata:
            self._recover_metadata_tree(spark)

        return spark

    def _recover_metadata_tree(self, spark: SparkSession) -> None:
        """
        Scans the warehouse directory and registers existing Delta tables
        into the Spark Metastore.
        """
        self.log_message("STARTING METADATA TREE RECOVERY", start=True, sep="=")

        if not self._warehouse_path.exists():
            self.log_message(f"Warehouse path {self._warehouse_path} does not exist. Skipping recovery.")
            return

        # Find all .db directories
        db_paths = list(self._warehouse_path.glob("*.db"))

        for db_path in db_paths:
            # Extract DB name (e.g., 'sales.db' -> 'sales')
            db_name = db_path.stem

            self.log_message(f"{db_name}", start=True, sep="-")

            # 1. Create Database
            # We use the absolute path to ensure Spark knows exactly where to look
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_path.as_posix()}'")

            # 2. Find tables within the database directory
            # We filter for directories only, assuming every dir inside a .db is a table
            table_paths = [p for p in db_path.iterdir() if p.is_dir()]

            for i, table_path in enumerate(table_paths):
                table_name = table_path.name

                # Tree visual logic
                is_last = (i == len(table_paths) - 1)
                prefix = "└──" if is_last else "├──"

                # 3. Create Table
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {db_name}.{table_name} USING DELTA LOCATION '{table_path.as_posix()}'")

                self.log_message(f"  {prefix} {table_name}")

        self.log_message("METADATA TREE RECOVERY | COMPLETED", start=True, sep="=")

    @staticmethod
    def get_dbs_tables(spark: SparkSession) -> Dict[str, List[str]]:
        """
        Returns a dictionary of {database: [list_of_tables]} currently registered in Spark.
        """
        # Get databases (excluding default)
        databases = [
            row.namespace for row in spark.sql("SHOW DATABASES").collect()
            if row.namespace != 'default'
        ]

        # Build dictionary
        return {
            db: [row.tableName for row in spark.sql(f"SHOW TABLES IN {db}").collect()]
            for db in databases
        }

    @staticmethod
    def _print_banner():
        """Prints the ASCII art banner."""
        # --------------------------------------------------------------------------------------------------------------
        print(f"┌{'─' * 118}┐")
        print(r"_______/\\\\\\\\\\\____/\\\\\\\\\\\\\_______/\\\\\\\\\_______/\\\\\\\\\______/\\\________/\\\___________________________")
        print(r"______/\\\/////////\\\_\/\\\/////////\\\___/\\\\\\\\\\\\\___/\\\///////\\\___\/\\\_____/\\\//___________________________")
        print(r"______\//\\\______\///__\/\\\_______\/\\\__/\\\/////////\\\_\/\\\_____\/\\\___\/\\\__/\\\//_____________________________")
        print(r"________\////\\\_________\/\\\\\\\\\\\\\/__\/\\\_______\/\\\_\/\\\\\\\\\\\/____\/\\\\\\//\\\____________________________")
        print(r"____________\////\\\______\/\\\/////////____\/\\\\\\\\\\\\\\\_\/\\\//////\\\____\/\\\//_\//\\\__________________________")
        print(r"________________\////\\\___\/\\\_____________\/\\\/////////\\\_\/\\\____\//\\\___\/\\\____\//\\\________________________")
        print(r"__________/\\\______\//\\\__\/\\\_____________\/\\\_______\/\\\_\/\\\_____\//\\\__\/\\\_____\//\\\____............______")
        print(r"__________\///\\\\\\\\\\\/___\/\\\_____________\/\\\_______\/\\\_\/\\\______\//\\\_\/\\\______\//\\\__............._____")
        print(r"_____________\///////////_____\///______________\///________\///__\///________\///__\///________\///__............._____")
        print(r"│                                                             ........      '.....................................     │")
        print(r"│                                                            .........        ...................................      │")
        print(r"│                                                            .........         ..................   ...........        │")
        print(r"│                                                       .............           '........         ...........          │")
        print(r"│                                                ....................                           ...........            │")
        print(r"│                                         ...........................                         ...........              │")
        print(r"│                                     ............................                          ............               │")
        print(r"│                                   .......................                               ............                 │")
        print(r"│                                  .................                                    ............                   │")
        print(r"│                                  ...................                                  ...........                    │")
        print(r"│                                   ........................                             ...........                   │")
        print(r"│                                     '..........................                         ...........                  │")
        print(r"___/\\\\\\\\\\\\____________________/\\\\\\________________________________________________..........___________________")
        print(r"___\/\\\////////\\\_________________\////\\\_________________________________________________.........._________________")
        print(r"____\/\\\______\//\\\___________________\/\\\________/\\\_____________________________________..........._______________")
        print(r"_____\/\\\_______\/\\\_____/\\\\\\\\_____\/\\\_____/\\\\\\\\\\\__/\\\\\\\\\_________......_____...........______________")
        print(r"______\/\\\_______\/\\\___/\\\/////\\\____\/\\\____\////\\\////__\////////\\\_______....................... ____________")
        print(r"_______\/\\\_______\/\\\__/\\\\\\\\\\\_____\/\\\_______\/\\\________/\\\\\\\\\\_____........................____________")
        print(r"________\/\\\_______/\\\__\//\\///////______\/\\\_______\/\\\_/\\___/\\\/////\\\____........................____________")
        print(r"_________\/\\\\\\\\\\\\/____\//\\\\\\\\\\__/\\\\\\\\\____\//\\\\\___\//\\\\\\\\/\\__........................____________")
        print(r"_________\////////////_______\//////////__\/////////______\/////_____\////////\//________................. _____________")
        # --------------------------------------------------------------------------------------------------------------