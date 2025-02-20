"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

@dag(
    dag_id = "elt_gupy",
    start_date = datetime(2025, 2, 18),
    schedule = "@daily",
    catchup = False
    tags = ["elt"]
)
def elt_gupy():
#Execução --------------------------------------------------------------------------

@task
def extract():
    url = f"https://portal.api.gupy.io/api/job?name={label}&offset={offset}&limit=400"
    r = requests.get(url)
    return r