# src/load.py

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from datetime import datetime

def load_to_bigquery(df: pd.DataFrame, table_name: str, partition_field: str = None, cluster_fields: list = None):
    print(f"\nUploading to: {table_name}")

    # Convert date columns for partitioning
    if partition_field and partition_field in df.columns:
        df[partition_field] = pd.to_datetime(df[partition_field], errors='coerce')

    
    try:
        client = bigquery.Client()

        table_id = f"rcm-project-467713.healthcare_rcm.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )

        # Partitioning
        if partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )

        # Clustering
        if cluster_fields:
            job_config.clustering_fields = cluster_fields

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Waits for job to finish
        print(f"✅ Successfully loaded to BigQuery table: {table_id}")

    except GoogleCloudError as e:
        print(f"❌ Failed to load to BigQuery: {e.message}")
    except Exception as ex:
        print(f"❌ Failed to load to BigQuery: {ex}")
