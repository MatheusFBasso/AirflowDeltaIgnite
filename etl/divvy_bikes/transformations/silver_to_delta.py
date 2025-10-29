from common.DeltaSpark import DeltaSpark
from silver import Silver
import inspect

if __name__ == "__main__":
    import sys
    args = sys.argv[1] if len(sys.argv) > 1 else ValueError('Argument 1 is required')

    if args not in [func[0] for func in inspect.getmembers(Silver)]:
        raise ValueError(f"Argument '{args}' is not a valid function!")

    Silver(spark=DeltaSpark(app_name=f'Airflow | DivvyBikes | Silver |{args}|').initialize()).__getattribute__(args)()