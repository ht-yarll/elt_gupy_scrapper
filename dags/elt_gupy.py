"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""
from utils.load_config import load_config
from typing import List, Dict, Any
import json

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from google.cloud import storage

import requests
from datetime import datetime

config = load_config()

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
    def extract() -> List[Dict[str, Any]]:
        # Extract jobs list from a Gupy URL

        offset = 0
        all_data = []
        label = config['labels']
        print(f'Fetching data for {label}...')

        try:
            while True:
                url_template = (
                    f"https://portal.api.gupy.io/api/job?name={label}&offset={offset}&limit=400"
                    )
                print(f'Fetching page {offset}...')

                response = requests.get(url_template)
                data = response.json()
                
                all_data.extend(data['data'])

                if not data['data']:
                    break

                offset += 10

            result = all_data

            local_file = f"/tmp/all_jobs.json"
            with open(local_file, "w", encoding="utf-8") as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)

            return local_file
           
        except Exception as e:
            print(f'Failed to fetch data: {e}')
            return []



    @task
    def load_raw_to_gcs(local_file: str) -> None:
        # Load data to GCS
        if not local_file:
            print("No file to upload.")
            return
        
        bucket_name = config['storage']['bucket_name']
        destination_blob_name = "all_jobs.json"

        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_filename(local_file)

            print(f"File {local_file} uploaded to {bucket_name}/{destination_blob_name}.")
        except Exception as e:
            print(f"Error uploading file {local_file}: {e}")
    
    load_raw_to_gcs(extract())

elt_gupy()