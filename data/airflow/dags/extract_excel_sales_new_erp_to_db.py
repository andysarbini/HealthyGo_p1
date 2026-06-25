from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text, create_engine

# =====================================================================
# FUNGSI UTILITAS PEMBERSIHAN DATA
# =====================================================================
def clean_phone_number(val):
    if pd.isna(val):
        return ""
    str_val = str(val).strip()
    if str_val.endswith('.0'):
        str_val = str_val[:-2]
    if 'e+' in str_val or 'E+' in str_val:
        try:
            str_val = str(int(float(str_val)))
        except ValueError:
            pass
    str_val = ''.join(filter(str.isdigit, str_val))
    if str_val.startswith('08'):
        str_val = '62' + str_val[1:]
    elif str_val.startswith('8'):
        str_val = '62' + str_val
    return str_val

def clean_bank_account_no(val):
    if pd.isna(val):
        return ""
    str_val = str(val).strip()
    if str_val.endswith('.0'):
        str_val = str_val[:-2]
    str_val = ''.join(filter(str.isdigit, str_val))
    return str_val.zfill(10)

def join_strings(x):
    valid_x = [str(i).strip() for i in x if pd.notna(i) and str(i).strip() != '']
    return ", ".join(valid_x) if valid_x else ""

def clean_name_for_merge(val):
    if pd.isna(val):
        return ""
    val = str(val).lower().strip()
    if '@' in val:
        val = val.split('@')[0]
    val = re.sub(r'[^a-z]', '', val)
    return val

# =====================================================================
# FUNGSI UTAMA ETL
# =====================================================================
def process_and_load_sales_new_erp():
    # 1. Konfigurasi File & Database Target
    EXCEL_FILE_PATH = '/opt/airflow/dags/data/sales_report_1782355726.xlsx' 
    TARGET_TABLE = 't_sales_new_erp'
    KOLOM_TANGGAL = 'payment_date'
    
    print(f"Membaca data dari {EXCEL_FILE_PATH}...")
    df_raw = pd.read_excel(EXCEL_FILE_PATH)
    df_raw = df_raw.rename(columns={'DOB': 'dob'})
    df_transformed = df_raw.copy()

    # ==========================================
    # TAHAP 1: FIXED DATA BLEEDING
    # ==========================================
    df_transformed['invoice_no'] = df_transformed['invoice_no'].fillna('').astype(str).str.strip()
    df_transformed['invoice_no'] = df_transformed['invoice_no'].apply(
        lambda x: x[:-2] if isinstance(x, str) and x.endswith('.0') else x
    )
    df_transformed['invoice_no'] = df_transformed['invoice_no'].replace(['', 'nan'], np.nan)
    df_transformed['invoice_no'] = df_transformed['invoice_no'].ffill()

    transaction_cols = [
        'id', 'upgrade_to', 'tanggal', 'waktu', 'branch', 'cust_name', 'dob', 
        'phone_number', 'email', 'gender', 'cs_status', 'status', 'total', 'kodeunik', 'amount', 
        'potongan_upgrade', 'payment_total', 'payer_acc_name', 'bank_account', 'bank_account_no', 
        'payment_date', 'trx_type', 'sales', 'purpose'
    ]

    print("Melakukan ffill pada kolom transaksi untuk mencegah data bleeding...")
    df_transformed[transaction_cols] = df_transformed.groupby('invoice_no')[transaction_cols].ffill()

    df_transformed['phone_number'] = df_transformed['phone_number'].apply(clean_phone_number)
    df_transformed['bank_account_no'] = df_transformed['bank_account_no'].apply(clean_bank_account_no)

    agg_dict = {col: 'first' for col in transaction_cols}
    for col in ['package', 'program', 'voucher_code', 'started_at']:
        agg_dict[col] = join_strings
    for col in ['qty_meal', 'qty_juice', 'qty_cutlery']:
        agg_dict[col] = 'sum'

    print("Melakukan agregasi (Grouping)...")
    df_final = df_transformed.groupby('invoice_no', as_index=False).agg(agg_dict)

    # ==========================================
    # INISIALISASI INCREMENTAL DATE (H-1)
    # ==========================================
    target_date = (datetime.now() - timedelta(days=1)).date()
    target_date_str = target_date.strftime('%Y-%m-%d')
    print(f"Batas tanggal cutoff (H-1) adalah: {target_date_str}")

    # ==========================================
    # KONEKSI DATABASE & PROSES MERGE
    # ==========================================
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    uri = pg_hook.get_uri()
    
    # Bypass bug Airflow URI (__extra__)
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    clean_uri = uri.split('?')[0]
    engine = create_engine(clean_uri)

    with engine.begin() as connection:
        
        # TAHAP 2: HAPUS DATA LAMA (BULK DELETE >= H-1)
        # Menggantikan metode loop hapus duplikat yang lama
        print(f"Menghapus data di tabel '{TARGET_TABLE}' dengan {KOLOM_TANGGAL} >= {target_date_str}...")
        delete_query = text(f"DELETE FROM {TARGET_TABLE} WHERE DATE({KOLOM_TANGGAL}) >= :target_date")
        connection.execute(delete_query, {"target_date": target_date_str})

        # TAHAP 3: MERGE DENGAN T_SALES_DIVISION
        print("Mengambil dan memproses data referensi 't_sales_division'...")
        query_division = "SELECT team, staff FROM t_sales_division"
        df_division = pd.read_sql(query_division, con=connection) 
        
        df_final['merge_key'] = df_final['sales'].apply(clean_name_for_merge)
        df_division['merge_key'] = df_division['team'].apply(clean_name_for_merge)
                
        df = pd.merge(df_final, df_division, on='merge_key', how='left')
        
        cols_to_drop = ['merge_key', 'team']
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

        if 'staff' in df.columns:
            df = df.rename(columns={'staff': 'division'})

        if 'cs_status' in df.columns:
            df['cs_status'] = df['cs_status'].astype(str).str.strip().str.lower()

        # Logika Kondisi cs_status
        kondisi = [
            (df['division'] == "R - SCS") & (df['cs_status'].isin(['repeat', 'comeback'])),
            (df['division'] == "R - SCS") & (df['cs_status'] == "new")
        ]
        pilihan_nilai = ["R - Success (SCS)", "R - Acquisition (SCS)"]
        df['division'] = np.select(kondisi, pilihan_nilai, default=df['division'])    

        # Logika general_division
        kondisi_general = [
            df['division'].isin(["R - Acquisition", "R - Acquisition (B2B)", "R - Acquisition (SCS)"]),
            df['division'].isin(["R - Success", "R - Success (B2B)", "R - Success (SCS)"])
        ]
        pilihan_general = ["Acquisition", "Customer Service"]
        df['general_division'] = np.select(kondisi_general, pilihan_general, default="Misc")
        
        # TAHAP 4: MERGE DENGAN T_SALES_TEAM
        print("Mengambil dan memproses data referensi 't_sales_team'...")
        query_team = "SELECT sales, team FROM t_sales_team"
        df_team = pd.read_sql(query_team, con=connection)
        
        df_team.columns = df_team.columns.str.lower().str.replace(' ', '_')
        
        if 'sales' in df.columns:
            df['merge_key'] = df['sales'].apply(clean_name_for_merge)
        if 'sales' in df_team.columns:
            df_team['merge_key'] = df_team['sales'].apply(clean_name_for_merge)
        
        df = pd.merge(df, df_team[['merge_key', 'team']], on='merge_key', how='left')    
    
        if 'merge_key' in df.columns:
            df = df.drop(columns=['merge_key'])
        if 'team' in df.columns:
            df = df.rename(columns={'team': 'teams'})

        # TAHAP 5: FILTER DATA EXCEL & INJEKSI
        # Pastikan kolom tanggal bertipe datetime untuk proses pemfilteran
        df[KOLOM_TANGGAL] = pd.to_datetime(df[KOLOM_TANGGAL], errors='coerce')
        
        # Saring hanya data dari excel yang >= H-1
        df_filtered = df[df[KOLOM_TANGGAL] >= pd.to_datetime(target_date_str)].copy()
        print(f"Jumlah baris Excel yang akan diinput (>= {target_date_str}): {len(df_filtered)} baris.")

        if df_filtered.empty:
            print("Tidak ada data baru untuk diinsert. Proses selesai.")
        else:
            # Mengubah format datetime Pandas menjadi string untuk PostgreSQL 
            # (Menghindari error NaT / Not a Time)
            for col in df_filtered.select_dtypes(include=['datetime64', 'datetimetz']).columns:
                df_filtered[col] = df_filtered[col].dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', None)
            
            # Menghindari NaN text di PostgreSQL dengan convert ke tipe None asli Python
            df_filtered = df_filtered.replace({np.nan: None, 'nan': None})
            
            print(f"Menyimpan data hasil akhir ke tabel '{TARGET_TABLE}'...")
            df_filtered.to_sql(
                name=TARGET_TABLE, 
                con=connection, 
                if_exists='append', 
                index=False 
            )
            print(f"✅ Sukses! Data ERP Sales berhasil diperbarui di tabel '{TARGET_TABLE}'.")

# =====================================================================
# DEFINISI DAG AIRFLOW
# =====================================================================
default_args = {
    'owner': 'andy_sarbini',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='extract_sales_new_erp_to_db',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_load_sales = PythonOperator(
        task_id='load_sales_erp_data',
        python_callable=process_and_load_sales_new_erp
    )

    task_load_sales