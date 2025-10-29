from common.DeltaSpark import DeltaSpark
from bronze import Bronze

if __name__ == "__main__":
    import sys
    divvy_path = sys.argv[1] if len(sys.argv) > 1 else ValueError('Argument 1 is required')
    Bronze(spark=DeltaSpark(app_name=f'Airflow | DivvyBikes | Bronze | {divvy_path} |').initialize()).load_raw_data_to_bronze(divvy_path=divvy_path)