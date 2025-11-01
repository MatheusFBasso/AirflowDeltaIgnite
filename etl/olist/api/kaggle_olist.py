import requests
import base64
import zipfile
import os
from typing import Optional
from common.utils import Now
from glob import glob


class KaggleDatasetDownloader(Now):
    """
    A class to download and extract datasets from Kaggle using the requests library.

    This class handles authentication with Kaggle API tokens, constructs the API URL,
    downloads the dataset as a ZIP file, and extracts it to a specified directory.
    Inherits from Now for logging capabilities.

    Attributes:
        token_path (str): Path to the kaggle.json file containing API credentials.
        username (str): Kaggle username (loaded from token).
        key (str): Kaggle API key (loaded from token).
    """

    _SHOW = True

    def __init__(self, token_path: str = 'etl/olist/api/kaggle_key.json', show: bool = True):
        """
        Initializes the KaggleDatasetDownloader with the path to the API token file and logging visibility.

        Args:
            token_path (str, optional): Path to kaggle.json. Defaults to 'kaggle.json'.
            show (bool, optional): Whether to show log messages. Defaults to True.

        Raises:
            FileNotFoundError: If the token file does not exist.
            ValueError: If the token file is malformed.
        """

        print(r"                                                                                                                        ")
        print(r"          8 8888     ,88'          .8.           ,o888888o.        ,o888888o.    8 8888         8 8888888888            ")
        print(r"          8 8888    ,88'          .888.         8888     `88.     8888     `88.  8 8888         8 8888                  ")
        print(r"          8 8888   ,88'          :88888.     ,8 8888       `8. ,8 8888       `8. 8 8888         8 8888                  ")
        print(r"          8 8888  ,88'          . `88888.    88 8888           88 8888           8 8888         8 8888                  ")
        print(r"          8 8888 ,88'          .8. `88888.   88 8888           88 8888           8 8888         8 888888888888          ")
        print(r"          8 8888 88'          .8`8. `88888.  88 8888           88 8888           8 8888         8 8888                  ")
        print(r"          8 888888<          .8' `8. `88888. 88 8888   8888888 88 8888   8888888 8 8888         8 8888                  ")
        print(r"          8 8888 `Y8.       .8'   `8. `88888.`8 8888       .8' `8 8888       .8' 8 8888         8 8888                  ")
        print(r"          8 8888   `Y8.    .888888888. `88888.  8888     ,88'     8888     ,88'  8 8888         8 8888                  ")
        print(r"          8 8888     `Y8. .8'       `8. `88888.  `8888888P'        `8888888P'    8 888888888888 8 888888888888          ")
        print(r"                                                                                                                        ")
        print(r"______________/\\\\\\\\\_____/\\\\\\\\\\\\\____/\\\\\\\\\\\________________/\\\___________/\\\\\\\______________________")
        print(r"_____________/\\\\\\\\\\\\\__\/\\\/////////\\\_\/////\\\///_____________/\\\\\\\_________/\\\/////\\\___________________")
        print(r"_____________/\\\/////////\\\_\/\\\_______\/\\\_____\/\\\_______________\/////\\\________/\\\____\//\\\_________________")
        print(r"_____________\/\\\_______\/\\\_\/\\\\\\\\\\\\\/______\/\\\___________________\/\\\_______\/\\\_____\/\\\________________")
        print(r"______________\/\\\\\\\\\\\\\\\_\/\\\/////////________\/\\\___________________\/\\\_______\/\\\_____\/\\\_______________")
        print(r"_______________\/\\\/////////\\\_\/\\\_________________\/\\\___________________\/\\\_______\/\\\_____\/\\\______________")
        print(r"________________\/\\\_______\/\\\_\/\\\_________________\/\\\___________________\/\\\_______\//\\\____/\\\______________")
        print(r"_________________\/\\\_______\/\\\_\/\\\______________/\\\\\\\\\\\_______________\/\\\__/\\\__\///\\\\\\\/______________")
        print(r"__________________\///________\///__\///______________\///////////________________\///__\///_____\///////_______________")

        super().__init__(show=show)
        self.token_path = token_path
        self.username, self.key = self._load_token()

    def _load_token(self) -> tuple:
        """
        Loads the Kaggle API token from the specified file.

        Returns:
            tuple: (username, key) from the token file.

        Raises:
            FileNotFoundError: If the token file is not found.
            ValueError: If the token does not contain 'username' and 'key'.
        """
        self.log_message(show=self._SHOW, message=f"GETTING TOKEN", start=True)
        if not os.path.exists(self.token_path):
            raise FileNotFoundError(f"Token file '{self.token_path}' not found.")

        with open(self.token_path, 'r') as f:
            import json
            token = json.load(f)

        username = token.get('username')
        key = token.get('key')
        if not username or not key:
            raise ValueError("Token file must contain 'username' and 'key'.")
        self.log_message(show=self._SHOW, message=f"GETTING TOKEN | OK", end=True)
        return username, key

    def _prepare_credentials(self) -> str:
        """
        Prepares base64-encoded credentials for Basic Auth.

        Returns:
            str: Base64-encoded string of 'username:key'.
        """
        self.log_message(show=self._SHOW, message=f"GETTING CREDENTIALS", start=True)
        creds = f"{self.username}:{self.key}"
        self.log_message(show=self._SHOW, message=f"GETTING CREDENTIALS | OK", end=True)
        return base64.b64encode(creds.encode()).decode()

    def download_dataset(
            self,
            owner_slug: str,
            dataset_slug: str,
            dataset_version: Optional[int] = None,
            extract_dir: str = 'data/raw_data/olist',
            zip_path: Optional[str] = None
    ) -> None:
        """
        Downloads and extracts a Kaggle dataset with logging.

        Args:
            owner_slug (str): The dataset owner's slug (e.g., 'olistbr').
            dataset_slug (str): The dataset slug (e.g., 'brazilian-ecommerce').
            dataset_version (int, optional): Specific version number. Defaults to latest.
            extract_dir (str, optional): Directory to extract files to. Defaults to '.'.
            zip_path (str, optional): Path to save the ZIP file. Defaults to '{dataset_slug}.zip'.

        Raises:
            requests.exceptions.RequestException: If the download fails.
            zipfile.BadZipFile: If the downloaded file is not a valid ZIP.
        """

        creds_encoded = self._prepare_credentials()

        base_url = "https://www.kaggle.com/api/v1"
        url = f"{base_url}/datasets/download/{owner_slug}/{dataset_slug}"
        if dataset_version:
            url += f"?datasetVersionNumber={dataset_version}"

        headers = {
            "Authorization": f"Basic {creds_encoded}",
            "Content-Type": "application/json"
        }

        self.log_message(show=self._SHOW, message=f"DOWNLOADING {dataset_slug}.zip", start=True)
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()  # Raise exception for HTTP errors

        if not zip_path:
            zip_path = f"{dataset_slug}.zip"

        # Save ZIP
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        self.log_message(show=self._SHOW, message=f"DOWNLOADING {dataset_slug}.zip | OK", end=True)

        self.log_message(show=self._SHOW, message=f"EXTRACTING {dataset_slug}", start=True)

        # Extract ZIP
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(extract_dir)
        self.log_message(show=self._SHOW, message=f"Extracted to {extract_dir}", sep='.')

        # Optional: List extracted files
        extracted_files = os.listdir(extract_dir)
        for _file in [x for x in extracted_files if x.endswith('.csv')]:
            self.log_message(show=self._SHOW, message=f"{_file}", sep='.-.')

        self.log_message(show=self._SHOW, message=f"EXTRACTING {dataset_slug} | OK", end=True)

if __name__ == '__main__':
    KaggleDatasetDownloader().download_dataset(owner_slug='olistbr', dataset_slug='brazilian-ecommerce')

