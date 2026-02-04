import inspect


from ghibli.apis.ghibli import GhibliAPI


def func_not_found(func: str) -> None:
    """Raise an exception for an unknown function name."""
    raise Exception(f"Function {func} not found.")


class GhibliCall:

    def __new__(cls, function: str, *args, **kwargs):
        """
        Dispatch the call to the appropriate static method based on function name.

        Args:
            function (str): The name of the function to call (e.g., 'get_raw_data').
            *args: Variable positional arguments passed to the target method.
            **kwargs: Variable keyword arguments passed to the target method.

        Returns:
            The result from the dispatched method.

        Raises:
            Exception: If the function name is not found.
        """
        map_function = {
            'get_raw_data': cls.get_raw_data_aux,
        }

        func = map_function.get(function)
        if func is None:
            func_not_found(function)

        return func(args, kwargs)

    @staticmethod
    def get_raw_data_aux(args: tuple, kwargs: dict):
        """
        Auxiliary method to fetch raw data from the DivvyBikes API.

        This method initializes the DivvyBikes API with the provided 'func' keyword.

        Args:
            args (tuple): Positional arguments (not used).
            kwargs (dict): Keyword arguments, expecting 'func'.

        Returns:
            The result of DivvyBikes().initialize().

        Raises:
            KeyError: If 'func' is not provided in kwargs (handled by .get()).
        """
        return GhibliAPI(kwargs.get('endpoint')).initialize()