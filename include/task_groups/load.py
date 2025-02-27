from airflow.decorators import task
from airflow.models.baseoperator import chain_linear
from airflow.utils.task_group import TaskGroup

from google.cloud import storage, bigquery


class LoadData(TaskGroup):
    """
    Load file 'tmp/all_data.json' to GCS and create a Stage Table
    """
    def __init__(self, config, group_id = 'Load', tooltip = 'Load Job', **kwargs):
        super().__init__(group_id = group_id, tooltip = tooltip, **kwargs)

        self.config = config

        @task(task_group = self)
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


        @task(task_group = self)
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
                print('stage table ready to query')

            except Exception as e:
                print(f"Error creating table: {e}")

            
        load_task = load_raw_to_gcs(self.config['local_file'])
        stage_task = set_stage_table()

        chain_linear(load_task, stage_task)