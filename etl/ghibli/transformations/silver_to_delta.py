from common.DeltaSpark import DeltaSpark
from silver import Silver
import inspect

if __name__ == "__main__":
    import sys
    args = sys.argv[1] if len(sys.argv) > 1 else ValueError('Argument 1 is required')

    Silver(spark=DeltaSpark(app_name=f'Airflow | Ghibli | Silver | {args} |').initialize()).initialize(endpoint=args)