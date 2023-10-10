import os
import logging
import csv
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

with (DAG(dag_id='ecgdataprocessing',
          description='ecg data processing with schedule',
          schedule_interval='*/2 * * * *',  # "*/2 * * * *" At every 2nd minute.
          start_date=datetime(2023, 10, 9), catchup=False)
      as dag):
    @task
    def copy_ecg_data():
        destination_path = f"/Users/pavanmantha/Pavans/PracticeExamples/apache_airflow_tutorial/data{datetime.now()}.csv"
        logging.info('Write Dir')
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        logging.info(f'File={destination_path}')

        src_file = "/Users/pavanmantha/Pavans/PracticeExamples/DataScience_Practice/Advanced-ML/ECGCvdata.csv"
        with open(file=destination_path, mode='w') as fd2:
            writer = csv.writer(fd2)
            with open(file=src_file, mode='r') as src_file:
                reader = csv.reader(src_file)
                header = next(reader)
                writer.writerow(header)
                for row in reader:
                    writer.writerow(row)
        logging.info(f'Data written to File={destination_path}')


    def log_info():
        logging.info('WRITING NOW')
        logging.info(datetime.utcnow().isoformat())
        logging.info('TIME WRITTEN')


    logger_task = PythonOperator(task_id='print_now', python_callable=log_info, dag=dag)

    logger_task >> copy_ecg_data()
