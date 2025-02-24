"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""
import os

from utils.load_config import config
from typing import List, Dict, Any
import json

from airflow import Dataset
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from google.cloud import storage, bigquery

import requests
from datetime import datetime



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

#EXTRACT ----------------------------------------------------------------------------
    @task
    def extract(label: str) -> str:
        """
        Extract jobs list from a Gupy URL and parse it to json
        """

        offset = 0
        all_data = []

        print(f'Fetching data for {label}...')

        try:
            while True:
                url_template = (
                    f"https://portal.api.gupy.io/api/job?name={label}&offset={offset}&limit=400"
                    )
                print(f'Fetching page {offset}...')

                response = requests.get(url_template)
                data = response.json()

                if not data['data']:
                    break

                all_data.extend(data['data'])
                offset += 10

        except Exception as e:
            print(f'Failed to fetch data: {e}')
        
        return all_data
    
    @task
    def merge_and_save(data_batches: List[List[dict]]) -> str:
        """
        Merge data from multiple labes, remove duplicates and saves in a single file  
        """

        local_file = '/tmp/all_jobs.json'
        existing_ids = set()
        merged_data = []

        if os.path.exists(local_file):
            with open(local_file, "r", encoding="utf-8") as f:
                for line in f:
                    job = json.loads(line.strip())
                    existing_ids.add(job.get("id"))
        
        for batch in data_batches:
            for job in batch:
                if job.get('id') not in existing_ids:
                    merged_data.append(job)
                    existing_ids.add(job.get("id"))
        
        with open(local_file, "a", encoding="utf-8") as f:
            for job in merged_data:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")

        print(f'Saved {len(merged_data)} jobs to {local_file}')
        
    @task_group
    def extract_all_labels():
        labels = config['labels']
        extract_task = [extract(label) for label in labels]
        return merge_and_save(extract_task)


#LOAD --------------------------------------------------------------------------
    @task
    def load_raw_to_gcs(local_file: str) -> None:
        """
        Load the extracted data .json to GCS
        """

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


    @task
    def set_stage_table() -> None:
        """
        Create a stage table from raw data loaded to gcs in BigQuery
        """
        uri = f"{config['storage']['bucket_uri']}/all_jobs.json"
        table_name = config['BigQuery']['stage']['table_name']
        job_config = bigquery.LoadJobConfig(
                    source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    autodetect = True,
                    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                )

        try:
            bq_client = bigquery.Client()
            load_job = bq_client.load_table_from_uri(
                source_uris = [uri],
                destination = table_name,
                job_config = job_config
            )

            load_job.result()

        except Exception as e:
            print(f"Error creating table: {e}")

#TRANSFORM --------------------------------------------------------------------------
    @task
    def create_bronze_table() -> None:
        try:
            bq_client = bigquery.Client()
            load_job = bq_client.query(
                query = config['BigQuery']['bronze']['query']
            )

            load_job.result()

        except Exception as e:
            print (f"Error during query: {(e)}")
    
    bq_client = bigquery.Client()
    @task
    def create_silver_locations() -> None:
        try:
            load_job_location = bq_client.query(
                query = config['BigQuery']['silver']['gupy_jobs_location']['query']
            )

            load_job_location.result()

        except Exception as e:
            print (f"Error during query: {(e)}")

    @task
    def create_silver_jobs() -> None:
        try:   
            load_job_jobs = bq_client.query(
                query = config['BigQuery']['silver']['gupy_jobs_jobs']['query']
            )

            load_job_jobs.result()

        except Exception as e:
            print (f"Error during query: {(e)}")
    @task
    def create_silver_company_and_time() -> None:
        try:
            load_job_company_and_time = bq_client.query(
                query = config['BigQuery']['silver']['gupy_jobs_company_and_time']['query']
            )

            load_job_company_and_time.result()

        except Exception as e:
            print (f"Error during query: {(e)}")

    @task
    def create_gold_for_analysis() -> None:
        load_job_gold = bq_client.query(
            query = config['BigQuery']['gold']['query']
        )

        load_job_gold.result()

    with TaskGroup("silver_tables") as silver_tables:
        create_silver_locations()
        create_silver_jobs()
        create_silver_company_and_time()
    


#callout tasks --------------------------------------------------------------------------    
    raw_data = extract_all_labels()
    data_to_gcs = load_raw_to_gcs(raw_data)

    data_to_gcs >> set_stage_table() >> create_bronze_table() >> silver_tables >> create_gold_for_analysis()
        

elt_gupy()