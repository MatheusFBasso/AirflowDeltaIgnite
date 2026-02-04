import os
import json
import requests
from common.utils import Now
from ghibli.utils.paths import raw_bronze as RAW_BRONZE


class GhibliAPI(Now):
    """
    An ETL handler for extracting data from the Studio Ghibli API and
    persisting it into a Bronze (Raw) data layer.

    Inherits from the Now utility class to provide standardized logging
    and timestamping capabilities across the pipeline.
    """

    _SHOW_LOG = True
    _ENDPOINTS = ['films', 'people', 'locations', 'species', 'vehicles']
    _BASE_URL = 'https://ghibliapi.vercel.app/'

    def __init__(self, endpoint: str) -> None:
        """
        Initializes the GhibliAPI extractor with a specific endpoint.

        Args:
            endpoint (str): The Ghibli API resource to fetch (e.g., 'films', 'people').

        Raises:
            ValueError: If the provided endpoint is not supported by the API.
        """
        super().__init__()
        endpoint = endpoint.lower()
        if endpoint not in self._ENDPOINTS:
            raise ValueError(f"Endpoint '{endpoint}' not supported. Choose from {self._ENDPOINTS}")

        self.endpoint = endpoint
        self.save_path = os.path.join(RAW_BRONZE, self.endpoint)

    def _get_ghibli_data(self) -> list:
        """
        Performs the 'Extract' phase of the ETL.
        Fetches raw JSON data from the specified Studio Ghibli endpoint.

        Returns:
            list: A list of dictionaries representing the API entities.

        Raises:
            requests.exceptions.HTTPError: If the API request fails.
        """
        self.log_message(show=self._SHOW_LOG, message=f"Starting extraction for: {self.endpoint}", start=True)

        resp = requests.get(f"{self._BASE_URL}{self.endpoint}")
        resp.raise_for_status()

        self.log_message(show=self._SHOW_LOG, message=f"Extraction successful: {self.endpoint}", end=True)
        return resp.json()

    def _save_to_bronze(self, data: list) -> None:
        """
        Performs the 'Load' phase of the ETL for the Bronze layer.
        Persists raw data to a JSON file with a timestamped filename.

        Args:
            data (list): The JSON data to be saved.
        """
        self.log_message(show=self._SHOW_LOG, message="BEGINNING TO SAVE DATA TO RAW LAYER")

        # Generate unique filename using inherited timestamp logic
        timestamp = self.now_datetime().strftime("%Y_%m_%dT%H_%M")
        file_name = f"extracted_at_{timestamp}.json"
        full_path = os.path.join(self.save_path, file_name)

        # Ensure the landing directory exists
        os.makedirs(self.save_path, exist_ok=True)

        # Atomic write of JSON data
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        self.log_message(
            show=self._SHOW_LOG,
            message=f"DATA SAVED SUCCESSFULLY. FILE: {file_name}",
        )

    def initialize(self) -> None:
        """
        Orchestrates the ETL process.
        This is the primary entry point for executing the pipeline flow:
        1. Fetch data from API.
        2. Save data to the local file system (Bronze layer).
        """
        try:
            json_data = self._get_ghibli_data()
            self._save_to_bronze(data=[{'type': self.endpoint, 'data': json_data, "ttl": len(json_data), "version": 1.0, "last_updated": self.now_datetime().timestamp()}])
        except Exception as e:
            self.log_message(show=True, message=f"PIPELINE FAILED: {str(e)}")
            raise