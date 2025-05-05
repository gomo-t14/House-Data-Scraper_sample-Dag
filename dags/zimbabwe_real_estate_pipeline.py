
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Define PostgreSQL connection info (ideally use Airflow Connections instead of hardcoding)
#test data
POSTGRES_CONN = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': '1234'
}

def load_data(**kwargs):
    df = pd.read_json('C:\Users\Tendekayi Gomo\Desktop\Projects\HousingAnalysis\property_co_zw1.jsonl', lines=True)
    kwargs['ti'].xcom_push(key='raw_df', value=df.to_json())

def clean_data(**kwargs):
    import json
    df_json = kwargs['ti'].xcom_pull(key='raw_df')
    df = pd.read_json(df_json)

    df.replace("", pd.NA, inplace=True)
    df['bedrooms'] = df['bedrooms'].astype('float')
    df['Price_2025'] = df['Price_2025'].astype('float')
    df['Company'] = df['Company'].astype('string')
    df['Surbub'] = df['Surbub'].astype('string')
    df['City'] = df['City'].astype('string')
    df['Province'] = df['Province'].astype('string')
    df['Description'] = df['Description'].astype('string')
    df['Price_2024'] = df['Price_2024'].astype('float')
    df['building_area'] = pd.to_numeric(df['building_area'], errors='coerce')
    df['land_area'] = pd.to_numeric(df['land_area'], errors='coerce')
    df['Agent'] = df['Agent'].astype('string')

    kwargs['ti'].xcom_push(key='clean_df', value=df.to_json())

def drop_nulls(**kwargs):
    df_json = kwargs['ti'].xcom_pull(key='clean_df')
    df = pd.read_json(df_json)
    df = df.dropna()
    kwargs['ti'].xcom_push(key='final_df', value=df.to_json())

def upload_to_postgres(**kwargs):
    df_json = kwargs['ti'].xcom_pull(key='final_df')
    df = pd.read_json(df_json)

    conn_str = f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
    engine = create_engine(conn_str)
    df.to_sql('zimbabwe_real_estate', engine, if_exists='replace', index=False)

with DAG(
    dag_id='zimbabwe_real_estate_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='drop_nulls',
        python_callable=drop_nulls,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        provide_context=True
    )

    t1 >> t2 >> t3 >> t4
