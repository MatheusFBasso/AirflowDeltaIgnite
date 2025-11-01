from common.DeltaSpark import DeltaSpark
from bronze import Bronze

if __name__ == "__main__":
    Bronze(spark=DeltaSpark(app_name=f'Airflow | Olist | Bronze').initialize()).process_files()