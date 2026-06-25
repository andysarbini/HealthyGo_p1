from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, text

# --- FUNGSI PROSES DATA ---
def process_and_load_data():
    EXCEL_FILE_PATH = '/opt/airflow/dags/data/division.xlsx' 
    TARGET_TABLE = 't_sales_division'
    
    df = pd.read_excel(EXCEL_FILE_PATH)
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # --- MULAI PERUBAHAN ---
    # Ambil objek koneksi mentah dari Airflow
    conn_obj = pg_hook.get_connection('postgres_default')
    
    # Rakit URL Database secara manual untuk menghindari bug "__extra__"
    db_url = f"postgresql://{conn_obj.login}:{conn_obj.password}@{conn_obj.host}:{conn_obj.port}/{conn_obj.schema}"
    
    # Buat engine menggunakan db_url yang sudah bersih
    engine = create_engine(db_url)
    # --- AKHIR PERUBAHAN ---

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE} RESTART IDENTITY CASCADE;"))

    df.columns = df.columns.str.lower().str.replace(' ', '_')
    if 'team' in df.columns:
        df['team'] = df['team'].astype(str).str.lower()
    
    for kolom in df.columns:
        if df[kolom].apply(lambda x: isinstance(x, (dict, list))).any():
            df[kolom] = df[kolom].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    df.to_sql(name=TARGET_TABLE, con=engine, if_exists='append', index=False)

# --- DEFINISI DAG ---
default_args = {
    'owner': 'andy_sarbini',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='extract_division_to_db',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_load = PythonOperator(
        task_id='load_division_data',
        python_callable=process_and_load_data
    )

    task_load