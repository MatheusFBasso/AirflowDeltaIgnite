from common.DeltaSpark import DeltaSpark
from gold import Gold

if __name__ == "__main__":

    Gold(spark=DeltaSpark(app_name=f'Airflow | Brewery | Gold').initialize()).brewery_type_total()