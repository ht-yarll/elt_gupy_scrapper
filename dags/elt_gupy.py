"""
This dag scrape data from an URL and load it to a bucket on storage for further transformation
"""
from datetime import datetime

from include.utils.load_config import config
from include.task_groups.extract import ExtractData
from include.task_groups.load import LoadData
from include.task_groups.transform import TransformData

from airflow.decorators import dag

default_args = {
    "owner": "ht-yarll",
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

#Execution ----------------------------------------------------------------

#E-------------------------------------------------------------------------
    extract = ExtractData(config)
#L-------------------------------------------------------------------------
    load = LoadData(config)
#T-------------------------------------------------------------------------
    transform = TransformData(config)
    
#callout tasks ------------------------------------------------------------
    extract >> load >> transform
        
elt_gupy()