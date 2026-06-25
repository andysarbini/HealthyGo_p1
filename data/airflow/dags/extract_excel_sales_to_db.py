import os
import json
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ==========================================
# KONFIGURASI DEFAULT DAG
# ==========================================
default_args = {
    'owner': 'andy_sarbini',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==========================================
# TAHAP 1: EXTRACT & TRANSFORM
# ==========================================
def extract_transform_sales(**kwargs):
    file_path = '/opt/airflow/dags/data/Report Sales All Branch Monday, 01 June 2026 - Thursday, 25 June 2026 By Payment Date Status success .xlsx'
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File Excel tidak ditemukan di {file_path}. Pastikan file sudah diunggah.")
    
    print(f"Membaca file dari {file_path}...")
    df = pd.read_excel(file_path)
    
    print("Memulai Transformasi Data...")
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    df = df.loc[:, ~df.columns.duplicated()]
    
    df = df.replace(r'^\s*$', np.nan, regex=True)
    
    if 'meal' in df.columns:
        df['meal'] = pd.to_numeric(df['meal'], errors='coerce')

    aturan_agregasi = {kolom: 'first' for kolom in df.columns if kolom != 'invoice'}
    
    if 'paket' in df.columns:
        aturan_agregasi['paket'] = lambda x: ', '.join(x.dropna().astype(str).unique())
    if 'meal' in df.columns:
        aturan_agregasi['meal'] = 'sum'

    df = df.groupby('invoice', dropna=False, as_index=False).agg(aturan_agregasi)
    
    for kolom in df.columns:
        if df[kolom].apply(lambda x: isinstance(x, (dict, list))).any():
            df[kolom] = df[kolom].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            
    df['sales'] = df['sales'].astype(str).str.lower().str.strip()
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print("Menarik data referensi t_sales_division...")
    df_division = pg_hook.get_pandas_df("SELECT team, staff FROM t_sales_division")
    
    print("Menarik data referensi t_sales_team...")
    df_team = pg_hook.get_pandas_df("SELECT sales, team FROM t_sales_team")
    df_team.columns = df_team.columns.str.lower().str.replace(' ', '_')
    if 'sales' in df_team.columns:
        df_team['sales'] = df_team['sales'].astype(str).str.strip().str.lower()
        
    df = pd.merge(df, df_division, left_on='sales', right_on='team', how='left')
    if 'team' in df.columns:
        df = df.drop(columns=['team'])
    if 'staff' in df.columns:
        df = df.rename(columns={'staff': 'division'})
        
    if 'cs_status' in df.columns:
        df['cs_status'] = df['cs_status'].astype(str).str.strip().str.lower()
        
    kondisi = [
        (df['division'] == "R - SCS") & (df['cs_status'].isin(['repeat', 'comeback'])),
        (df['division'] == "R - SCS") & (df['cs_status'] == "new")
    ]
    pilihan_nilai = ["R - Success (SCS)", "R - Acquisition (SCS)"]
    df['division'] = np.select(kondisi, pilihan_nilai, default=df['division'])    
    
    kondisi_general = [
        df['division'].isin(["R - Acquisition", "R - Acquisition (B2B)", "R - Acquisition (SCS)"]),
        df['division'].isin(["R - Success", "R - Success (B2B)", "R - Success (SCS)"])
    ]
    pilihan_general = ["Acquisition", "Customer Service"]
    df['general_division'] = np.select(kondisi_general, pilihan_general, default="Misc")
    
    if 'phone' in df.columns:
        df['phone_clean'] = df['phone'].astype(str).str.strip().replace('nan', '')
        df['phone_clean'] = df['phone_clean'].str.replace(r'\D', '', regex=True)
        df['phone_clean'] = df['phone_clean'].str.replace(r'^0', '62', regex=True)     
        df['phone_clean'] = df['phone_clean'].str.replace(r'^8', '628', regex=True)
        df['phone_clean'] = df['phone_clean'].replace('', np.nan)
        
    df = pd.merge(df, df_team, on='sales', how='left')    
    df['timestamp'] = pd.Timestamp.now()    
    
    for col in df.select_dtypes(include=['datetime64', 'datetimetz']).columns:
        df[col] = df[col].astype(str)
        
    print(f"Tahap 1 selesai. {len(df)} baris data siap dimuat.")
    return df.to_dict('records')

# ==========================================
# TAHAP 2: LOAD KE DATABASE POSTGRES
# ==========================================
def load_sales_to_db(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='extract_transform_task')
    
    if not records:
        print("Tidak ada data untuk dimasukkan. Proses dihentikan.")
        return
        
    df = pd.DataFrame(records)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    uri = pg_hook.get_uri()
    
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
        
    clean_uri = uri.split('?')[0]
    engine = create_engine(clean_uri)
    
    # --- PERBAIKAN LOGIKA INCREMENTAL LOAD ---
    # Asumsi kolom tanggal Anda bernama 'payment_date'.
    # Jika di Excel namanya lain (misal 'tanggal'), ubah variabel ini.
    KOLOM_TANGGAL = 'payment_date' 
    
    # Dapatkan tanggal saat ini dan kurangi 1 hari (H-1)
    # Contoh: Jika skrip jalan tgl 25, maka target_date = 24
    target_date = (datetime.now() - timedelta(days=1)).date()
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    print(f"Batas tanggal cutoff (H-1) adalah: {target_date_str}")
    
    # Kembalikan tipe kolom tanggal menjadi datetime untuk proses filter
    df[KOLOM_TANGGAL] = pd.to_datetime(df[KOLOM_TANGGAL], errors='coerce')
    
    # 1. Filter DataFrame untuk hanya mengambil data transaksi >= H-1
    df_filtered = df[df[KOLOM_TANGGAL] >= pd.to_datetime(target_date_str)]
    print(f"Jumlah data setelah difilter (>= {target_date_str}): {len(df_filtered)} baris.")
    
    if df_filtered.empty:
        print("Tidak ada data baru untuk diinsert. Proses selesai.")
        return

    # 2. Hapus data di database yang beririsan (>= H-1) untuk menghindari duplikasi
    delete_query = f"DELETE FROM t_sales WHERE DATE({KOLOM_TANGGAL}) >= '{target_date_str}';"
    
    try:
        print(f"Menghapus data lama dengan query: {delete_query}")
        pg_hook.run(delete_query)
    except Exception as e:
        print(f"Catatan/Error saat menghapus data: {e}")
        # Lanjutkan proses insert meskipun delete gagal (misal tabel masih kosong pertama kali)
    
    # 3. Masukkan data yang sudah difilter
    print("Menyimpan data baru (Incremental) ke database...")
    df_filtered.to_sql(
        name='t_sales', 
        con=engine, 
        if_exists='append', # Tetap gunakan append karena data lama tidak di-truncate
        index=False          
    )
    
    print("Tahap 2 selesai. Data sales berhasil diperbarui.")


# ==========================================
# DEFINISI DAG
# ==========================================
with DAG(
    'extract_sales_erp_to_db',
    default_args=default_args,
    description='Ekstraksi file Excel Sales, Transformasi, dan Load ke PostgreSQL (t_sales)',
    schedule_interval='30 2 * * *',
    start_date=datetime(2026, 6, 20),
    catchup=False,
    tags=['healthygo', 'erp', 'sales', 'excel'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_transform_task',
        python_callable=extract_transform_sales,
    )

    task_load = PythonOperator(
        task_id='load_db_task',
        python_callable=load_sales_to_db,
    )

    task_extract >> task_load