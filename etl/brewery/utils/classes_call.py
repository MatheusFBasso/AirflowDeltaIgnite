from brewery.apis.brewery_api import BreweryAPI


def func_not_found(func: str) -> None:
    """Raise an exception for an unknown function name."""
    raise Exception(f"Function {func} not found.")


class BreweryCall:

    def __new__(cls, function: str, *args, **kwargs):

        map_function = {
            'get_raw_data': cls.get_raw_data_aux,
        }

        func = map_function.get(function)
        if func is None:
            func_not_found(function)

        return func(args, kwargs)

    @staticmethod
    def get_raw_data_aux(args: tuple, kwargs: dict):
        return BreweryAPI().extract_data()