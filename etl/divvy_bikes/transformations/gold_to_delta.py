from common.DeltaSpark import DeltaSpark
from gold import Gold
import inspect

if __name__ == "__main__":
    import sys
    args = sys.argv[1] if len(sys.argv) > 1 else ValueError('Argument 1 is required')

    if args not in [func[0] for func in inspect.getmembers(Gold)]:
        raise ValueError(f"Argument '{args}' is not a valid function!")

    Gold(spark=DeltaSpark(app_name=f'Airflow | DivvyBikes | Gold |{args}|').initialize()).__getattribute__(args)()