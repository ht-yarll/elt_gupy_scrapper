"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""
# from utils.load_config import load_config

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime

from typing import List, Dict, Any
import requests

# config = load_config()

@dag(
    dag_id = "elt_gupy",
    start_date = datetime(2025, 2, 18),
    schedule = "@daily",
    catchup = False,
    doc_md = __doc__,
    default_args = {"owner": "ht-yarll", "retries": 3},
    tags = ["elt"]
)
def elt_gupy():
#Execution --------------------------------------------------------------------------

    @task
    def extract() -> List[Dict[str, Any]]:
        # Extract jobs list from a Gupy URL

        offset = 0
        all_data = []
        label = 'python'
        print(f'Fetching data for {label}...')

        try:
            while True:
                url_template = (
                    f"https://portal.api.gupy.io/api/job?name={label}&offset={offset}&limit=400"
                    )
                print(f'Fetching page {offset}...')

                response = requests.get(url_template)
                data = response.json()
                
                for i in data['data']:
                    all_data.append(i)

                if not data['data']:
                    break

                offset += 10
            
            result = all_data
            print('All data fetched with success')
            return result
           
        except Exception as e:
            print(f'Failed to fetch data: {e}')
            return []
        

    extract() >> 
    
elt_gupy()    