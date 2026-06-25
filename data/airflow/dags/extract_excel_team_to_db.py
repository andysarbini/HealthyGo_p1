from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text, create_engine

# --- FUNGSI PROSES DATA ---
def process_and_load_team():
    # Sesuaikan path file Excel untuk team
    EXCEL_FILE_PATH = '/opt/airflow/dags/data/DSI_Team.xlsx' 
    TARGET_TABLE = 't_sales_team'
    
    print(f"Membaca data dari {EXCEL_FILE_PATH}...")
    df = pd.read_excel(EXCEL_FILE_PATH)
    
    # Inisialisasi koneksi dengan cara yang aman (bypass bug __extra__)
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn_obj = pg_hook.get_connection('postgres_default')
    db_url = f"postgresql://{conn_obj.login}:{conn_obj.password}@{conn_obj.host}:{conn_obj.port}/{conn_obj.schema}"
    engine = create_engine(db_url)

    # Truncate tabel
    with engine.begin() as conn:
        print(f"Melakukan truncate pada tabel '{TARGET_TABLE}'...")
        conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE} RESTART IDENTITY CASCADE;"))

    # Transformasi kolom
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # Contoh transformasi spesifik jika diperlukan
    if 'team' in df.columns:
        df['team'] = df['team'].astype(str).str.lower()
    
    # Konversi tipe data kompleks ke JSON string
    for kolom in df.columns:
        if df[kolom].apply(lambda x: isinstance(x, (dict, list))).any():
            df[kolom] = df[kolom].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Load ke database
    print(f"Sedang mengirim data ke tabel '{TARGET_TABLE}'...")
    df.to_sql(name=TARGET_TABLE, con=engine, if_exists='append', index=False)
    print(f"✅ Sukses! Data berhasil disimpan ke tabel '{TARGET_TABLE}'.")

# --- DEFINISI DAG ---
default_args = {
    'owner': 'andy_sarbini',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='extract_team_to_db',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_load = PythonOperator(
        task_id='load_team_data',
        python_callable=process_and_load_team
    )

    task_load