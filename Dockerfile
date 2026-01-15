FROM apache/airflow:3.1.5
COPY requirements.txt .
RUN pip install apache-airflow==3.1.5 -r requirements.txt