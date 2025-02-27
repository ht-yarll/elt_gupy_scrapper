from airflow.decorators import task
from airflow.models.baseoperator import chain_linear
from airflow.utils.task_group import TaskGroup

from google.cloud import bigquery

class TransformData(TaskGroup):
    def __init__(self, config, group_id = 'Transform', tooltip = 'Transform Job', **kwargs):
        super().__init__(group_id = group_id, tooltip = tooltip, **kwargs)

        self.config = config
        bq_client = bigquery.Client()

        @task(task_group = self)
        def create_bronze_table() -> None:
            try:
                load_job_create = bq_client.query(
                    query = config['BigQuery']['bronze']['query']
                )

                load_job_drop = bq_client.query(
                    query = config['BigQuery']['bronze']['drop_query']
                )

                load_job_create.result()
                print("Table created successfully.")
                try:
                    load_job_drop.result()
                    print("Table dropped successfully.")

                except Exception as e:
                    print (f"Error during drop: {(e)}")  

            except Exception as e:
                print (f"Error during query: {(e)}")
       

        @task(task_group = self)
        def create_silver_locations() -> None:
            try:
                load_job_location = bq_client.query(
                    query = config['BigQuery']['silver']['gupy_jobs_location']['query']
                )

                load_job_location.result()
                print('location table created successfully')

            except Exception as e:
                print (f"Error during query: {(e)}")


        @task(task_group = self)
        def create_silver_jobs() -> None:
            try:   
                load_job_jobs = bq_client.query(
                    query = config['BigQuery']['silver']['gupy_jobs_jobs']['query']
                )

                load_job_jobs.result()
                print('jobs table created successfully')

            except Exception as e:
                print (f"Error during query: {(e)}")


        @task(task_group = self)
        def create_silver_company_and_time() -> None:
            try:
                load_job_company_and_time = bq_client.query(
                    query = config['BigQuery']['silver']['gupy_jobs_company_and_time']['query']
                )

                load_job_company_and_time.result()
                print('company_and_time table created successfully')

            except Exception as e:
                print (f"Error during query: {(e)}")


        @task(task_group = self)
        def create_gold_for_analysis() -> None:
            try:
                load_job_gold = bq_client.query(
                    query = config['BigQuery']['gold']['query']
                )

                load_job_gold.result()
                print('gold table created successfully')

            except Exception as e:
                print (f"Error during query: {(e)}")

        
        bronze = create_bronze_table()
        silver = [create_silver_locations(), create_silver_jobs(), create_silver_company_and_time()]
        gold = create_gold_for_analysis()

        chain_linear(
            bronze,
            silver,
            gold
        )