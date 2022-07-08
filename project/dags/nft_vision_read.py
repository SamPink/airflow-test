try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd
    import requests

    print("All Dag modules are ok ......")
except Exception as e:
    print(f"Error  {e} ")


class Reservoir:
    """
    This class handles the API calls to the reservoir API.
    """

    def __init__(self):
        self.base_url = "https://api.reservoir.tools"
        self.headers = {"Accept": "*/*", "x-api-key": "demo-api-key"}

    def get_top_collections(self):
        url = f"{self.base_url}/collections/v4?sortBy=30DayVolume&includeTopBid=false&limit=10"
        print(url)
        top_collections = requests.get(url, headers=self.headers).json()
        return top_collections["collections"]


def try_to_get_nft_data():
    reservoir = Reservoir()
    top_collections = reservoir.get_top_collections()

    print(top_collections)
    return top_collections


with DAG(
    dag_id="nft_vision_read",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1),
    },
    catchup=False,
) as f:

    test_01 = PythonOperator(
        task_id="test_01",
        python_callable=try_to_get_nft_data,
    )
