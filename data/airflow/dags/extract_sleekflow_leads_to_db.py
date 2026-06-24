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
# TAHAP 1: FUNGSI EXTRACT & TRANSFORM
# ==========================================
def extract_from_sleekflow(**kwargs):
    URL_API = "https://api.sleekflow.io/api/contact/dynamicSearch"
    api_key = Variable.get("sleekflow_api_key", default_var="xx") 
    
    wib_tz = timezone(timedelta(hours=7))
    h2 = datetime.now(wib_tz) - timedelta(days=2)
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

    all_contacts = []
    limit = 200
    offset = 0

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))

    def is_blank(val):
        return val is None or str(val).strip() == ""

    # Proses Ekstraksi
    while True:
        payload = {
            "conditions": [{"conditionOperator": "HigherThan", "fieldName": "CreatedAt", "values": [utc_start_str]}],
            "include": {"customfields": [], "labels": "true", "latestMessage": "true"},
            "pagination": {"limit": limit, "offset": offset},
            "sort": {"field": "createdAt", "order": "asc"}
        }

        response = session.post(URL_API, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])

        if not results:
            break

        for contact in results:
            raw_time = contact.get("createdAt")
            if raw_time:
                # Membuang seluruh pecahan detik beserta titiknya (contoh: .36022 atau .3397508)
                raw_time_fixed = re.sub(r'\.\d+', '', raw_time)
                
                # Sekarang string menjadi '2026-06-20T01:17:23+00:00' (sangat aman dibaca Python)
                dt = datetime.fromisoformat(raw_time_fixed.replace('Z', '+00:00'))
                contact['created_at_b'] = (dt + timedelta(hours=7)).isoformat()
            else:
                contact['created_at_b'] = None

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

            contact['assigned_team_original'] = assigned_team
            contact['assigned_team_update'] = assigned_team_update
            contact['contact_owner_id_update'] = contact_owner_id_update

            all_contacts.append(contact)

        print(f" > Offset {offset}: {len(results)} data berhasil diekstrak.")
        if len(results) < limit:
            break
        offset += limit

    # Persiapan data untuk dioper ke task selanjutnya
    hari_ini_wib = datetime.now(wib_tz)
    h_minus_1 = (hari_ini_wib - timedelta(days=1)).strftime('%Y-%m-%d')
    all_data_tuples = []

    for contact in all_contacts:
        created_at_b = contact.get("created_at_b")
        if created_at_b:
            tanggal_wib = created_at_b[:10]
            if tanggal_wib >= h_minus_1:
                lead_stage = next((cf.get('customValue') for cf in contact.get("customFields", []) if cf.get('customFieldName') == "LeadStage"), None)
                data_tuple = (
                    contact.get("id"), contact.get("firstName"), contact.get("phoneNumber"),
                    contact.get("createdAt"), contact.get("created_at_b"), contact.get("updatedAt"),
                    contact.get("lastContact"), contact.get("lastContactFromCustomers"), contact.get("contactOwnerId"),
                    contact.get("labels", []), contact.get("assigned_team_original"), lead_stage,
                    contact.get("assigned_team_update"), contact.get("contact_owner_id_update")
                )
                all_data_tuples.append(data_tuple)

    print(f"Tahap 1 selesai. {len(all_data_tuples)} data siap di-load.")
    
    # Return data agar Airflow menyimpannya di XCom
    return all_data_tuples

# ==========================================
# TAHAP 2: FUNGSI LOAD KE DATABASE
# ==========================================
def load_to_database(**kwargs):
    # Mengambil objek 'ti' (Task Instance) dari kwargs
    ti = kwargs['ti']
    
    # Menarik data yang dihasilkan oleh task 'extract_data_task' menggunakan XCom
    all_data_tuples = ti.xcom_pull(task_ids='extract_data_task')
    
    if not all_data_tuples:
        print("Tidak ada data baru yang ditarik dari tahap sebelumnya. Melewati proses insert.")
        return

    print(f"Memulai Tahap 2: Load {len(all_data_tuples)} data ke Database...")

    wib_tz = timezone(timedelta(hours=7))
    hari_ini_wib = datetime.now(wib_tz)
    h_minus_1 = (hari_ini_wib - timedelta(days=1)).strftime('%Y-%m-%d')

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    delete_query = "DELETE FROM t_sleekflow_leads WHERE created_at_b >= %s"
    
    insert_query = """
    INSERT INTO t_sleekflow_leads (
        id, first_name, phone_number, created_at, created_at_b, updated_at, 
        last_contact, last_contact_from_customers, contact_owner_id, 
        labels, assigned_team, lead_stage, assigned_team_update, contact_owner_id_update
    ) VALUES %s
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(delete_query, (h_minus_1,))
            baris_dihapus = cursor.rowcount
            print(f" > Berhasil membersihkan {baris_dihapus} baris data lama (>= {h_minus_1}).")

            execute_values(cursor, insert_query, all_data_tuples)

        conn.commit()
        print("Tahap 2 selesai. Semua data berhasil dimasukkan ke database.")


# ==========================================
# DEFINISI DAG & URUTAN TASK
# ==========================================
with DAG(
    'extract_sleekflow_leads_to_db',
    default_args=default_args,
    description='Ekstraksi Sleekflow (Task 1) dan Load ke Database (Task 2)',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 6, 20),
    catchup=False,
    tags=['healthygo', 'sleekflow', 'ingestion'],
) as dag:

    # Task 1
    task_extract = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_from_sleekflow,
    )

    # Task 2
    task_load = PythonOperator(
        task_id='load_data_task',
        python_callable=load_to_database,
    )

    # Menentukan urutan jalannya Task (Task 1 -> Task 2)
    task_extract >> task_load