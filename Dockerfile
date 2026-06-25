FROM apache/airflow:2.7.0-python3.10
RUN pip install --no-cache-dir requests pandas openpyxl