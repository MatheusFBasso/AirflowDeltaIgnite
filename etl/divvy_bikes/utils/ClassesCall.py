import inspect

from divvy_bikes.apis.DivvyBikesAPI import DivvyBikes
from divvy_bikes.transformations.bronze import Bronze
from divvy_bikes.transformations.silver import Silver


def func_not_found(func: str) -> None:
    """Raise an exception for an unknown function name."""
    raise Exception(f"Function {func} not found.")


class DivvyBikesCall:
    """
    A dispatcher class for handling various Divvy Bikes data processing stages.

    This class uses the __new__ method to dispatch calls to specific static
    methods based on the provided function name. It acts as a central entry
    point for operations like fetching raw data, applying bronze transformations,
    and executing silver layer methods.

    Usage example:
        # Get raw data
        DivvyBikesCall('get_raw_data', func='some_function')

        # Bronze transformation
        DivvyBikesCall('bronze_raw_data', divvy_path='/path/to/data')

        # Silver transformation
        DivvyBikesCall('silver', 'some_method_name')

    Raises:
        Exception: If the function name is not recognized.
        ValueError: If invalid arguments are provided for specific functions.
    """

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
            'bronze_raw_data': cls.bronze_aux,
            'silver': cls.silver_aux,
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
        return DivvyBikes().initialize(func=kwargs.get('func'))

    @staticmethod
    def bronze_aux(args: tuple, kwargs: dict):
        """
        Auxiliary method for bronze layer data transformation.

        Loads raw data to the bronze layer using the provided divvy_path.

        Args:
            args (tuple): Positional arguments (not used).
            kwargs (dict): Keyword arguments, expecting 'divvy_path'.

        Returns:
            bool: True if the operation succeeds.
        """
        bronze = Bronze()
        bronze.load_raw_data_to_bronze(divvy_path=kwargs.get('divvy_path'))
        return True

    @staticmethod
    def silver_aux(args: tuple, kwargs: dict):
        """
        Auxiliary method for silver layer data transformations.

        Executes a specified method on the Silver class.

        Args:
            args (tuple): Positional arguments, expecting the method name as args[0].
            kwargs (dict): Keyword arguments (not used).

        Returns:
            bool: True if the operation succeeds.

        Raises:
            IndexError: If no method name is provided in args.
            ValueError: If the method name is not a valid attribute of Silver.
            AttributeError: If the attribute is not callable.
        """
        if not args:
            raise ValueError("No method name provided.")
        method_name = args[0]

        # Get all attribute names of the Silver class
        attr_names = [name for name, _ in inspect.getmembers(Silver, inspect.isfunction)]

        if method_name not in attr_names:
            raise ValueError(f"Method '{method_name}' is not a valid function in Silver!")

        silver = Silver()
        method = getattr(silver, method_name)
        method()

        return True