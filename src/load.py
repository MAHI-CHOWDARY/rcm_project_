
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

def load_to_bigquery(df: pd.DataFrame, table_name: str, partition_field: str = None, cluster_fields: list = None, write_disposition: str = "WRITE_TRUNCATE"):
    print(f"\nUploading to: {table_name} ({write_disposition})")

    if partition_field and partition_field in df.columns:
        df[partition_field] = pd.to_datetime(df[partition_field], errors='coerce')

    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).fillna("")
    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].astype(bool)

    try:
        client = bigquery.Client()
        table_id = f"rcm-project-467713.healthcare_rcm.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True
        )

        if partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        if cluster_fields:
            job_config.clustering_fields = cluster_fields

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Successfully loaded to BigQuery: {table_id}")

    except GoogleCloudError as e:
        print(f"❌ BigQuery error: {e.message}")
    except Exception as ex:
        print(f"❌ Unexpected error: {ex}")
