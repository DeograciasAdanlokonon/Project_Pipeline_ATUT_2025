from etl_config import *
from datetime import timedelta
from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# operator args
default_args = {
  'owner': 'ThePythoner',
  'start_date': datetime(2026, 1, 20),
  'email': ['thepythoner.net@gmail.com'],
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
  'ETL_Books_Data',
  default_args=default_args,
  description='Books Data DAG',
  schedule=timedelta(days=1),
  catchup=False,
)

# Define the task named books_scraping to call the 'extract_from_scraping' function
scraping_books = PythonOperator(
  task_id='extract_scraping',
  python_callable=extract_data_from_books_to_scraped,
  dag=dag,
)

# Define the task named books_drive to call the 'extract_from_drive' function
drive_books = PythonOperator(
    task_id='extract_drive',
    python_callable=extract_data_from_google_drive_books_csv,
    dag=dag,
)

# Define the task named kaggle_books to call the 'extract_from_kaggle' function
kaggle_books = PythonOperator(
    task_id='extract_kaggle',
    python_callable=extract_data_from_kaggle_books,
    dag=dag,
)

# Define the task named loading to call the 'consolidation' function
loading = PythonOperator(
    task_id='consolidate',
    python_callable=consolidates_all_extracted_file,
    dag=dag,
)

# Define the task named loading_minio to call 'load_minio' function
load_minio = PythonOperator(
    task_id='load_to_minio',
    python_callable=load_books_to_minio,
    dag=dag,
)

scraping_books >> drive_books >> kaggle_books >> loading >> load_minio
