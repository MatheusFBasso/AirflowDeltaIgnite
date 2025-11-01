import inspect

from olist.api.kaggle_olist import KaggleDatasetDownloader
from olist.transformations.bronze import Bronze


def func_not_found(func: str) -> None:
    """Raise an exception for an unknown function name."""
    raise Exception(f"Function {func} not found.")


class OlistClassesCall:

    def __new__(cls, function: str, *args, **kwargs):

        map_function = {
            'get_raw_data': cls.get_raw_data_aux,
            'bronze_raw_data': cls.bronze_aux,
        }

        func = map_function.get(function)
        if func is None:
            func_not_found(function)

        return func(args, kwargs)

    @staticmethod
    def get_raw_data_aux(args: tuple, kwargs: dict):

        return KaggleDatasetDownloader().download_dataset(
            owner_slug=kwargs.get('owner_slug'),
            dataset_slug=kwargs.get('dataset_slug'),
            extract_dir=kwargs.get('extract_dir')
        )

    @staticmethod
    def bronze_aux(args: tuple, kwargs: dict):

        bronze = Bronze()
        bronze.process_files()
        return True
