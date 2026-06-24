import requests
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta, timezone
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

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
# FUNGSI BANTUAN: STANDARISASI NOMOR HP
# ==========================================
def format_phone_number(phone):
    if not phone:
        return None
    
    # Hapus semua karakter non-angka
    cleaned = re.sub(r'\D', '', str(phone))
    if not cleaned:
        return None
        
    # Standarisasi awalan menjadi 62
    if cleaned.startswith('08'):
        cleaned = '62' + cleaned[1:]
    elif cleaned.startswith('8'):
        cleaned = '62' + cleaned
        
    return cleaned

# ==========================================
# TAHAP 1: FUNGSI EXTRACT & TRANSFORM
# ==========================================
def extract_activechat_from_sleekflow(**kwargs):
    URL_API = "https://api.sleekflow.io/api/contact/dynamicSearch"
    api_key = Variable.get("sleekflow_api_key", default_var="xx") 
    
    wib_tz = timezone(timedelta(hours=7))
    h2 = datetime.now(wib_tz) - timedelta(days=5)
    TANGGAL_MULAI = h2.strftime('%Y-%m-%d')

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Sleekflow-Api-Key": api_key,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    print(f"Memulai Tahap 1: Ekstraksi Data dari {TANGGAL_MULAI} (WIB)...")

    target_date_obj = datetime.strptime(TANGGAL_MULAI, "%Y-%m-%d")
    utc_start_obj = target_date_obj - timedelta(hours=7)
    utc_start_str = utc_start_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    all_data_tuples = []
    limit = 200
    offset = 0

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))

    def is_blank(val):
        return val is None or str(val).strip() == ""

    # Setup Batas Aman H-1 untuk Transformasi
    hari_ini_wib = datetime.now(wib_tz)
    h_minus_1 = (hari_ini_wib - timedelta(days=1)).strftime('%Y-%m-%d')
    data_diabaikan = 0

    while True:
        payload = {
            "conditions": [{"conditionOperator": "HigherThan", "fieldName": "lastContactFromCustomers", "values": [utc_start_str]}],
            "include": {"customfields": [], "labels": "true", "latestMessage": "true"},
            "pagination": {"limit": limit, "offset": offset},
            "sort": {"field": "lastContactFromCustomers", "order": "asc"}
        }

        response = session.post(URL_API, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])

        if not results:
            break

        for contact in results:
            # --- 1. FORMAT WAKTU WIB ---
            raw_time = contact.get("lastContactFromCustomers")
            if raw_time:
                # Perbaikan RegEx agar Python 3.10 tidak error saat membaca pecahan detik
                raw_time_fixed = re.sub(r'\.\d+', '', raw_time)
                dt = datetime.fromisoformat(raw_time_fixed.replace('Z', '+00:00'))
                lastContactFromCustomers_b = (dt + timedelta(hours=7)).isoformat()
            else:
                lastContactFromCustomers_b = None

            # --- 2. EKSTRAKSI & KONDISI ---
            assigned_team = next((cf.get('customValue') for cf in contact.get("customFields", []) if cf.get('customFieldName') == "AssignedTeam"), None)
            contact_owner_id = contact.get("contactOwnerId")
            labels = contact.get("labels", [])

            assigned_team_update = assigned_team
            contact_owner_id_update = contact_owner_id

            if is_blank(assigned_team) and is_blank(contact_owner_id):
                assigned_team_update = "Broadcast"
                contact_owner_id_update = "Broadcast"
            elif not is_blank(contact_owner_id) and is_blank(assigned_team):
                assigned_team_update = "14419"

            label_texts = [str(lbl.get('name', lbl)).lower() if isinstance(lbl, dict) else str(lbl).lower() for lbl in labels]
            if any("bukan" in teks or "luar" in teks for teks in label_texts):
                assigned_team_update = "Bukan Prospect"

            # --- 3. EKSTRAKSI CUSTOM FIELDS & PEMBUATAN TUPLE ---
            if lastContactFromCustomers_b:
                tanggal_data = lastContactFromCustomers_b[:10]
                
                if tanggal_data >= h_minus_1:
                    c_fields = contact.get("customFields", []) or []
                    
                    def get_cf(name):
                        return next((cf.get('customValue') or cf.get('value') for cf in c_fields if cf.get('kolom') == name or cf.get('customFieldName') == name), None)

                    formatted_phone = format_phone_number(contact.get("phoneNumber"))
                    
                    data_tuple = (
                        contact.get("id"), # contact_id
                        contact.get("firstName"),
                        contact.get("lastName", ""),
                        formatted_phone,
                        contact.get("createdAt"),
                        contact.get("updatedAt"),
                        contact.get("lastContact"),
                        contact.get("lastContactFromCustomers"),
                        lastContactFromCustomers_b,
                        contact.get("contactOwnerId"),
                        contact.get("labels", []),
                        get_cf("Variable 1"),
                        get_cf("Variable 2"),
                        get_cf("LeadStage"),
                        get_cf("ContactOwner"),
                        assigned_team, # assigned_team as original
                        assigned_team_update,
                        get_cf("LastChannel"),
                        get_cf("LeadSource")
                    )
                    all_data_tuples.append(data_tuple)
                else:
                    data_diabaikan += 1

        print(f" > Offset {offset}: {len(results)} data berhasil diekstrak.")
        if len(results) < limit:
            break
        offset += limit

    print(f"Tahap 1 selesai. {len(all_data_tuples)} data siap di-load. ({data_diabaikan} data lama diabaikan).")
    
    # Oper data ke task Load melalui XCom
    return all_data_tuples

# ==========================================
# TAHAP 2: FUNGSI LOAD KE DATABASE
# ==========================================
def load_activechat_to_database(**kwargs):
    ti = kwargs['ti']
    all_data_tuples = ti.xcom_pull(task_ids='extract_activechat_task')
    
    if not all_data_tuples:
        print("Tidak ada data valid (>= H-1) yang ditarik dari tahap sebelumnya. Melewati proses insert.")
        return

    print(f"Memulai Tahap 2: Load {len(all_data_tuples)} data ke Database...")

    wib_tz = timezone(timedelta(hours=7))
    hari_ini_wib = datetime.now(wib_tz)
    h_minus_1 = (hari_ini_wib - timedelta(days=1)).strftime('%Y-%m-%d')

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    delete_query = "DELETE FROM t_sleekflow_activechat WHERE last_contact_from_customers_b >= %s"
    
    insert_query = """
    INSERT INTO t_sleekflow_activechat (
        contact_id, first_name, last_name, phone_number, created_at, updated_at, 
        last_contact, last_contact_from_customers, last_contact_from_customers_b,
        contact_owner_id, labels, variable_1, variable_2, lead_stage, contact_owner, 
        assigned_team, assigned_team_update, last_channel, lead_source
    ) VALUES %s;
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Hapus data lama untuk mencegah duplikasi
            cursor.execute(delete_query, (h_minus_1,))
            baris_dihapus = cursor.rowcount
            print(f" > Berhasil membersihkan {baris_dihapus} baris data lama (>= {h_minus_1}).")

            # Eksekusi Bulk Insert
            custom_template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            execute_values(cursor, insert_query, all_data_tuples, template=custom_template)

        conn.commit()
        print("Tahap 2 selesai. Semua data berhasil dimasukkan ke database PostgreSQL.")

# ==========================================
# DEFINISI DAG & URUTAN TASK
# ==========================================
with DAG(
    'extract_sleekflow_activechat_to_db',
    default_args=default_args,
    description='Ekstraksi Sleekflow Active Chat (Task 1) dan Load ke DB (Task 2)',
    schedule_interval='15 2 * * *', # Dijalankan setiap 02:15 agar tidak bentrok dengan DAG leads di 02:00
    start_date=datetime(2026, 6, 20),
    catchup=False,
    tags=['healthygo', 'sleekflow', 'activechat'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_activechat_task',
        python_callable=extract_activechat_from_sleekflow,
    )

    task_load = PythonOperator(
        task_id='load_activechat_task',
        python_callable=load_activechat_to_database,
    )

    # Menentukan urutan jalannya Task
    task_extract >> task_load