"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""
# from utils.load_config import load_config

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
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
    def extract():
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
            
            result = pd.DataFrame(all_data)
            print('All data fetched with success')
            return result
           
        except Exception as e:
            print(f'Failed to fetch data: {e}')
            return pd.DataFrame()
        
    @task
    def normalize_data():
        df = extract()
        try:
            print('Treating data...')
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[^a-z0-9_]", "_", regex=True)

            )
            df = df.map(
            lambda x: str(x) if isinstance(x, dict) else x
        )
            df = df.fillna({
                col: "N/A" if df[col].dtype == "object" else 0
                for col in df.columns
            })
            df = df.drop_duplicates()
            df = df.convert_dtypes()
            
        except Exception as e:
            print(f'Error during treating data: {e}')

        return df


    extract() >> normalize_data()
    
elt_gupy()    