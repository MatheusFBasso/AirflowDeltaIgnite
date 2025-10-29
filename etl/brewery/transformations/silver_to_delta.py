from common.DeltaSpark import DeltaSpark
from silver import Silver

if __name__ == "__main__":

    Silver(spark=DeltaSpark(app_name=f'Airflow | Brewery | Silver').initialize()).execute()