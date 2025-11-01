import json
import os
from datetime import datetime

import requests

from common.utils import Now
from divvy_bikes.utils.paths import raw_bronze as RAW_BRONZ


class DivvyBikes(Now):
    """
    A class for interacting with the Divvy Bikes API, fetching data from various
    endpoints, and saving it to the bronze layer.

    This class inherits from Now (presumably providing utilities like log_message).
    It provides methods to dynamically fetch and save data based on specified patterns
    corresponding to API endpoints.

    Attributes:
        _SHOW_LOG (bool): Flag to control logging visibility.
        BRONZE_PATH_RAW_DATA (str): Path to the bronze raw data directory.
        DIVVY_BIKES_REF_DICT (dict): Dictionary mapping patterns to API URLs.
        STATION_STATUS (str): URL for station status.
        STATION_INFORMATION (str): URL for station information.
        FREE_BIKE_STATUS (str): URL for free bike status.
        SYSTEM_PRICING_PLANS (str): URL for system pricing plans.
        VEHICLE_TYPES (str): URL for vehicle types.

    Raises:
        ValueError: If an invalid pattern is provided for data fetching.
    """

    _SHOW_LOG = True

    BRONZE_PATH_RAW_DATA = RAW_BRONZ

    DIVVY_BIKES_REF_DICT = {
        'bike_status': {
            'URL': 'https://gbfs.lyft.com/gbfs/2.3/chi/en/free_bike_status.json',
        },
        'station_information': {
            'URL': 'https://gbfs.lyft.com/gbfs/2.3/chi/en/station_information.json',
        },
        'station_status': {
            'URL': 'https://gbfs.lyft.com/gbfs/2.3/chi/en/station_status.json',
        },
        'system_pricing_plan': {
            'URL': 'https://gbfs.lyft.com/gbfs/2.3/chi/en/system_pricing_plans.json',
        },
        'vehicle_types': {
            'URL': 'https://gbfs.lyft.com/gbfs/2.3/chi/en/vehicle_types.json',
        },
    }

    STATION_STATUS = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/station_status.json'
    STATION_INFORMATION = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/station_information.json'
    FREE_BIKE_STATUS = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/free_bike_status.json'
    SYSTEM_PRICING_PLANS = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/system_pricing_plans.json'
    VEHICLE_TYPES = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/vehicle_types.json'

    def __init__(self):
        """
        Initialize the DivvyBikes instance and print a welcome banner.
        """
        print(''''┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐''')
        print('''│                                                                                                                      │''')
        print('''│                                                                                                                      │''')
        print('''│      ██████████    ███                                        ███████████   ███  █████                               │''')
        print('''│     ░░███░░░░███  ░░░                                        ░░███░░░░░███ ░░░  ░░███                                │''')
        print('''│      ░███   ░░███ ████  █████ █████ █████ █████ █████ ████    ░███    ░███ ████  ░███ █████  ██████   █████          │''')
        print('''│      ░███    ░███░░███ ░░███ ░░███ ░░███ ░░███ ░░███ ░███     ░██████████ ░░███  ░███░░███  ███░░███ ███░░           │''')
        print('''│      ░███    ░███ ░███  ░███  ░███  ░███  ░███  ░███ ░███     ░███░░░░░███ ░███  ░██████░  ░███████ ░░█████          │''')
        print('''│      ░███    ███  ░███  ░░███ ███   ░░███ ███   ░███ ░███     ░███    ░███ ░███  ░███░░███ ░███░░░   ░░░░███         │''')
        print('''│      ██████████   █████  ░░█████     ░░█████    ░░███████     ███████████  █████ ████ █████░░██████  ██████          │''')
        print('''│     ░░░░░░░░░░   ░░░░░    ░░░░░       ░░░░░      ░░░░░███    ░░░░░░░░░░░  ░░░░░ ░░░░ ░░░░░  ░░░░░░  ░░░░░░           │''')
        print('''│     ."         d"  ^b.    *c        .$"  d"   $  ███ ░███                                                            │''')
        print('''│    /          P      $.    "c      d"   @     3r░░██████3        █████████   ███████████  █████                      │''')
        print('''│   4        .eE........$r===e$$$$eeP    J       *.░░░░░░  b      ███░░░░░███ ░░███░░░░░███░░███                       │''')
        print('''│   $       $$$$$       $   4$$$$$$$    F       d$$$.      4     ░███    ░███  ░███    ░███ ░███                       │''')
        print('''│   $       $$$$$       $   4$$$$$$$    L       *$$$"      4     ░███████████  ░██████████  ░███                       │''')
        print('''│   4         "      ""3P ===$$$$$$"    3                  P     ░███░░░░░███  ░███░░░░░░   ░███                       │''')
        print('''│    *                 $       """       b                J      ░███    ░███  ░███         ░███                       │''')
        print('''│     ".             .P                   %.             @       █████   █████ █████        █████ VERSION 1.0          │''')
        print('''│       %.         z*"                     ^%.        .r"       ░░░░░   ░░░░░ ░░░░░        ░░░░░                       │''')
        print('''│          "*==*""                            ^"*==*""                                                                 │''')
        print('''└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘''')

    def _save_file(
        self,
        save_data: list,
        path: str,
        file_name: str = "extracted_at_",
    ) -> None:
        """
        Save the provided data to a JSON file in the specified path.

        The file name is appended with the current timestamp in the format
        '%Y_%m_%dT%H_%M'. The data is saved as a JSON array.

        Args:
            save_data (list): List of dictionaries or data to save.
            path (str): Directory path where the file will be saved.
            file_name (str, optional): Base name of the file. Defaults to 'extracted_at_'.

        Returns:
            None
        """
        self.log_message(show=self._SHOW_LOG, message="BEGINNING TO SAVE THE DATA TO BRONZE LAYER")

        file_name += datetime.today().strftime("%Y_%m_%dT%H_%M")

        if not os.path.exists(f"{path}/"):
            os.makedirs(f"{path}/")

        with open(f"{path}/{file_name}.json", "w") as output:
            output.write("[" + ",\n".join(json.dumps(_dict) for _dict in save_data) + "]\n")

        self.log_message(
            show=self._SHOW_LOG,
            message="DATA SAVED SUCCESSFULLY. FILE NAME: {}".format(file_name),
        )

    def get_files_dymanic(self, pattern: str) -> bool:
        """
        Fetch data from the API based on the given pattern and save it to the bronze layer.

        Validates the pattern against known endpoints, fetches the JSON data, and saves it.

        Args:
            pattern (str): The key for the API endpoint (e.g., 'bike_status').

        Returns:
            bool: True if the operation succeeds (implicit return).

        Raises:
            ValueError: If the pattern is invalid.
        """
        self.log_message(show=self._SHOW_LOG, message="VALIDATING PATTERN", start=True)
        if pattern not in self.DIVVY_BIKES_REF_DICT.keys():
            raise ValueError(
                f"Pattern {pattern} is not a valid pattern. {self.DIVVY_BIKES_REF_DICT.keys()}"
            )
        self.log_message(show=self._SHOW_LOG, message="VALIDATING PATTERN | OK", start=True)

        FOLDER_NAME = pattern.replace("_", " ").upper()

        self.log_message(show=self._SHOW_LOG, message=f"GETTING {FOLDER_NAME}", start=True)
        data = requests.get(url=self.DIVVY_BIKES_REF_DICT.get(pattern).get("URL")).json()
        self.log_message(show=self._SHOW_LOG, message=f"GETTING {FOLDER_NAME} | OK", end=True)

        self._save_file(
            save_data=[data], path=f"{self.BRONZE_PATH_RAW_DATA}/{pattern}", file_name="extracted_at_"
        )

    def initialize(self, func: str):
        """
        Initialize data fetching by calling get_files_dymanic with the provided function/pattern.

        Args:
            func (str): The pattern/key for the API endpoint to fetch.
        """
        self.get_files_dymanic(pattern=func)