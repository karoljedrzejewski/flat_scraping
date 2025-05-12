from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/scripts/scraper')
from flat_data_scraping import FlatScraper
from example_spark import analyse_with_pyspark



# Konfiguracja domyślna DAGa
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Definicja DAG
dag = DAG(
    dag_id='flat_data_scraper',
    default_args=default_args,
    description='DAG for scraping flat data and analyzing it with PySpark',
    schedule='@daily',
    catchup=False
)

def scrape_olx(**context):
    scraper = FlatScraper()
    context['ti'].xcom_push(key='olx_data', value=scraper.scrap_olx())

def scrape_ot(**context):
    scraper = FlatScraper()
    context['ti'].xcom_push(key='ot_data', value=scraper.scrap_ot())

def combine_and_analyze(**context):
    olx_data = context['ti'].xcom_pull(key='olx_data', task_ids='scrape_olx') or []
    ot_data = context['ti'].xcom_pull(key='ot_data', task_ids='scrape_ot') or []
    data = olx_data + ot_data
    
    analyse_with_pyspark(data, f"data_{datetime.now().strftime('%Y_%m_%d')}")

scrape_olx_task = PythonOperator(
    task_id='scrape_olx',
    python_callable=scrape_olx,
    dag=dag
)

scrape_ot_task = PythonOperator(
    task_id='scrape_ot',
    python_callable=scrape_ot,
    dag=dag
)

analyze_task = PythonOperator(
    task_id='combine_and_analyze',
    python_callable=combine_and_analyze,
    dag=dag
)

[scrape_olx_task, scrape_ot_task] >> analyze_task