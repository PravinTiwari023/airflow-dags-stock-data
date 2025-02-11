from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def fetch_stock_data(**kwargs):
    # Fetch 15 years of historical data
    ticker = "NVDA"
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=15*365)).strftime('%Y-%m-%d')
    
    data = yf.download(
        tickers=ticker,
        start=start_date,
        end=end_date,
        interval="1d"
    ).reset_index()
    
    # Upload to GCS
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    csv_data = data.to_csv(index=False)
    
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    hook.upload(
        bucket_name='nvidia-stock-raw-data',
        object_name=f'nvidia_stock/{kwargs["ds"]}_data.csv',
        data=csv_data,
        mime_type='text/csv'
    )

with DAG(
    dag_id='nvidia_stock_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_nvidia_data',
        python_callable=fetch_stock_data
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='nvidia-stock-raw-data',
        source_objects=['nvidia_stock/*.csv'],
        destination_project_dataset_table='your_project.stock_data.raw',
        autodetect=True,
        write_disposition='WRITE_APPEND',
        time_partitioning={'type': 'DAY', 'field': 'Date'}
    )

    transform_data = BigQueryExecuteQueryOperator(
        task_id='transform_data',
        sql='''
            CREATE OR REPLACE TABLE your_project.stock_analytics.summary AS
            SELECT 
                Date,
                Close,
                AVG(Close) OVER (ORDER BY Date ROWS 365 PRECEDING) AS moving_avg
            FROM your_project.stock_data.raw
        ''',
        use_legacy_sql=False
    )

    fetch_task >> load_to_bq >> transform_data