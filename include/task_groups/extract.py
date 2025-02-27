import os
from typing import List
import json

from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import requests


class ExtractData(TaskGroup):
    """
    Extract jobs list from a Gupy URL and parse it to json
    """
    def __init__(self, config, group_id = 'Extraction', tooltip = 'Extraction Job', **kwargs):
        super().__init__(group_id = group_id, tooltip = tooltip, **kwargs)

        self.labels = config.get('labels', [])
                         
        @task(task_group = self)
        def extract(label: str) -> List[dict]:
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
        
        @task(task_group=self)
        def merge_and_save(data_batches: List[List[dict]]) -> str:
            """
            Merge data from multiple labes, remove duplicates and saves in a single file  
            """
            local_file = config['local_file']
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

            return local_file
            
        extract_tasks = extract.expand(label = self.labels)
        merge_and_save(data_batches=extract_tasks)
