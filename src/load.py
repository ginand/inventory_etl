from __future__ import annotations

import os
import pandas as pd
from google.cloud import bigquery


def ensure_dataset_exists(client: bigquery.Client, project_id: str, dataset_name: str) -> None:
    dataset_id = f"{project_id}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
        print(f"Dataset exists: {dataset_id}")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = os.environ.get("BQ_LOCATION", "US")
        client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset: {dataset_id}")


def load_dataframe_to_bq(
    df: pd.DataFrame,
    table_name: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    if df.empty:
        print(f"Skip loading {table_name}: dataframe is empty")
        return

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset_name = os.environ["BQ_DATASET"]
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    client = bigquery.Client(project=project_id)

    ensure_dataset_exists(client, project_id, dataset_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
    )
    job.result()

    print(f"Loaded {len(df)} rows into {table_id}")