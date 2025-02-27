"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""
from typing import List, Dict

from include.utils.load_config import config
from include.task_groups.extract import ExtractData
from include.task_groups.load import LoadData

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from google.cloud import storage, bigquery
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
    extract = ExtractData(config)

#LOAD --------------------------------------------------------------------------
    load = LoadData(config)

#TRANSFORM --------------------------------------------------------------------------
    @task(trigger_rule = "all_done")
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
   

    extract >> load >> create_bronze_table() >> silver_tables >> create_gold_for_analysis()
        

elt_gupy()