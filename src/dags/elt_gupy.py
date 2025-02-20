"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""
# from utils.load_config import load_config

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import pandas as pd
import requests
from datetime import datetime

# config = load_config()

default_args = {
    "owner": "astro",
    "depends_on_past": False,
    "retries": 3
}

@dag(
    dag_id = "elt_gupy",
    start_date = datetime(2025, 2, 18),
    schedule = "@daily",
    catchup = False,
    doc_md = __doc__,
    default_args = default_args,
    tags = ["elt"]
)
def elt_gupy():
#Execution --------------------------------------------------------------------------

    @task
    def extract() -> pd.DataFrame:
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
            print (result)
            return result
           
        except Exception as e:
            print(f'Failed to fetch data: {e}')
            return pd.DataFrame()
        
    @task
    def normalize_data():
        dataframe = extract()
        try:
            print('Treating data...')
            dataframe.columns = (
                dataframe.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[^a-z0-9_]", "_", regex=True)

            )
            dataframe = dataframe.map(
            lambda x: str(x) if isinstance(x, dict) else x
        )
            dataframe = dataframe.fillna({
                col: "N/A" if df[col].dtype == "object" else 0
                for col in df.columns
            })
            df = dataframe.drop_duplicates()
            df = df.convert_dtypes()
            
        except Exception as e:
            print(f'Error during treating data: {e}')

        return df
    
    @task
    def check_if_file_exists():
        # task to check if exist file in bkt
        check_file = GCSObjectExistenceSensor(
            task_id='check_file_exists',
            bucket='elt_gupy_scrapper',  # Replace with your bucket name
            object='all_data.parquet',  # Replace with the file path in the bucket
            timeout=300,  # Maximum wait time in seconds
            poke_interval=30,  # Time interval in seconds to check again
            mode='poke',  # Use 'poke' mode for synchronous checking
        )

    @task
    def upload_file_to_gcs():
        # task to upload file to bq
        upload_file = GCSToBigQueryOperator(
                task_id='load_parquet_to_bq',
                bucket='elt_gupy_scrapper',  # Replace with your bucket name
                source_objects=['all_data.parquet'],  # Path to your file in the bucket
                destination_project_dataset_table='blackstone-446301.elt_gupy_scrapper.all_data_raw',  # Replace with your project, dataset, and table name
                source_format='Parquet', 
                allow_jagged_rows=True,
                ignore_unknown_values=True,
                write_disposition='WRITE_APPEND',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
                skip_leading_rows=1,  # Skip header row
                autodetect=True,  # Automatically infer schema from the file
                #google_cloud_storage_conn_id='google_cloud_default',  # Uncomment and replace if custom GCP connection
                #bigquery_conn_id='google_cloud_default',  # Uncomment and replace if custom BigQuery connection
           
        )


    [extract() >> normalize_data()] >> check_if_file_exists() >> upload_file_to_gcs()

elt_gupy()