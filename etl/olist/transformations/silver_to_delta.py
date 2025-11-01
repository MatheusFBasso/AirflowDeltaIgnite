from common.DeltaSpark import DeltaSpark
from silver import Silver


if __name__ == "__main__":
    import sys
    args = sys.argv[1] if len(sys.argv) != 1 else ValueError('Argument 1 is required')

    Silver(spark=DeltaSpark(app_name=f'Airflow | Olist | Silver | {args} |').initialize()).run_silver(args)