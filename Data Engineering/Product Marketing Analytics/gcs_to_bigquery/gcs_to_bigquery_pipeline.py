from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

BUCKET_NAME = 'excel-data-bucket'
LOCAL_TMP_PATH = '/tmp'

FILES = {
    'user_registration.xlsx': 'raw.user_registration_staging',
    'product_usage_events.xlsx': 'raw.product_usage_events_staging',
    'subscription_data.xlsx': 'raw.subscription_data_staging',
    'marketing_campaign_data.xlsx': 'raw.marketing_campaign_data_staging',
}

def convert_all_excels_to_csv(**kwargs):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    for file_name in FILES.keys():
        if not file_name.endswith('.xlsx'):
            continue
        blob = bucket.blob(file_name)
        local_excel_path = os.path.join(LOCAL_TMP_PATH, file_name)
        blob.download_to_filename(local_excel_path)

        df = pd.read_excel(local_excel_path)
        csv_file_name = file_name.replace('.xlsx', '.csv')
        local_csv_path = os.path.join(LOCAL_TMP_PATH, csv_file_name)
        df.to_csv(local_csv_path, index=False)

        blob_csv = bucket.blob(csv_file_name)
        blob_csv.upload_from_filename(local_csv_path)

        os.remove(local_excel_path)
        os.remove(local_csv_path)

with DAG('full_gcs_to_bq_star_schema_pipeline_with_dim_date',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         description='Load Excel files to BigQuery staging, create star schema including dim_date',
         tags=['gcs', 'bigquery', 'automation']) as dag:

    # Step 1: Convert Excel to CSV in GCS
    convert_all = PythonOperator(
        task_id='convert_excels_to_csvs',
        python_callable=convert_all_excels_to_csv
    )

    # Step 2: Load CSV files into raw staging tables
    load_tasks = []
    for file_name, staging_table in FILES.items():
        csv_name = file_name.replace('.xlsx', '.csv')
        load_task = GCSToBigQueryOperator(
            task_id=f'load_{staging_table.replace(".", "_")}',
            bucket=BUCKET_NAME,
            source_objects=[csv_name],
            destination_project_dataset_table=staging_table,
            skip_leading_rows=1,
            source_format='CSV',
            autodetect=True,
            write_disposition='WRITE_TRUNCATE',
        )
        convert_all >> load_task
        load_tasks.append(load_task)

    # Step 3a: Create dim_date table (for example: from 2010-01-01 to 2030-12-31)
    create_dim_date = BigQueryInsertJobOperator(
        task_id="create_dim_date",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE analytics.dim_date AS
                WITH date_range AS (
                    SELECT
                        DATE('2010-01-01') + INTERVAL x DAY AS date
                    FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF('2030-12-31', '2010-01-01', DAY))) AS x
                )
                SELECT
                    date AS date_key,
                    EXTRACT(YEAR FROM date) AS year,
                    EXTRACT(QUARTER FROM date) AS quarter,
                    EXTRACT(MONTH FROM date) AS month,
                    EXTRACT(DAY FROM date) AS day,
                    FORMAT_DATE('%A', date) AS day_name,
                    FORMAT_DATE('%B', date) AS month_name,
                    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
                FROM date_range
                ORDER BY date;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Step 3b: Create star schema tables from staging
    create_dim_user = BigQueryInsertJobOperator(
        task_id="create_dim_user",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE analytics.dim_user AS
                    SELECT DISTINCT
                      user_id,
                      signup_date,
                      registration_source,
                      country,
                      city,
                      age_group,
                      gender
                    FROM raw.user_registration_staging;
                """,
                "useLegacySql": False,
            }
        },
    )

    create_fact_product_usage_events = BigQueryInsertJobOperator(
        task_id="create_fact_product_usage_events",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE analytics.fact_product_usage_events AS
                    SELECT
                      event_id,
                      user_id,
                      event_timestamp,
                      event_type,
                      feature_name,
                      duration_seconds,
                      session_id
                    FROM raw.product_usage_events_staging;
                """,
                "useLegacySql": False,
            }
        },
    )

    create_fact_subscriptions = BigQueryInsertJobOperator(
        task_id="create_fact_subscriptions",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE analytics.fact_subscriptions AS
                    SELECT
                      subscription_id,
                      user_id,
                      plan_type,
                      start_date,
                      end_date,
                      billing_frequency,
                      cancelation_date,
                      cancelation_reason,
                      mrr_value
                    FROM raw.subscription_data_staging;
                """,
                "useLegacySql": False,
            }
        },
    )

    create_fact_marketing_campaigns = BigQueryInsertJobOperator(
        task_id="create_fact_marketing_campaigns",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE analytics.fact_marketing_campaigns AS
                    SELECT
                      campaign_id,
                      campaign_name,
                      campaign_start_date,
                      campaign_end_date,
                      acquisition_channel,
                      campaign_type,
                      total_cost,
                      impressions,
                      clicks
                    FROM raw.marketing_campaign_data_staging;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Set dependencies for transformations
    for t in load_tasks:
        if 'user_registration_staging' in t.task_id:
            t >> create_dim_user
        elif 'product_usage_events_staging' in t.task_id:
            t >> create_fact_product_usage_events
        elif 'subscription_data_staging' in t.task_id:
            t >> create_fact_subscriptions
        elif 'marketing_campaign_data_staging' in t.task_id:
            t >> create_fact_marketing_campaigns

    # dim_date can run after all loads (or right after convert_all if you prefer)
    convert_all >> create_dim_date
