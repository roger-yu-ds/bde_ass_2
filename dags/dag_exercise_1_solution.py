import os
import logging
from datetime import (
    datetime, 
	timedelta
)

#########################################################
#
#   Load Environment Variables
#
#########################################################

#AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
#AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


########################################################
#
#   DAG Settings
#
#########################################################

from airflow import DAG

dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now() - timedelta(days=1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='exercise_1',
    default_args=dag_default_args,
    schedule_interval='@hourly',
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################


def print_bde():
    logging.info("Big Data Engineering")

def addition():
    logging.info(f"1 + 1 = {1+1}")

def subtraction():
    logging.info(f"6 -2 = {6-2}")

def division():
    logging.info(f"10 / 2 = {int(20/2)}")


#########################################################
#
#   DAG Operator Setup
#
#########################################################

from airflow.operators.python_operator import PythonOperator


print_bde_task = PythonOperator(
    task_id="print_bde_task_id",
    python_callable=print_bde,
    dag=dag)

addition_task = PythonOperator(
    task_id="addition_task_id",
    python_callable=addition,
    dag=dag)

subtraction_task = PythonOperator(
    task_id="subtraction_task_id",
    python_callable=subtraction,
    dag=dag)

division_task = PythonOperator(
    task_id="division_task_id",
    python_callable=division,
    dag=dag)

print_bde_task >> addition_task
print_bde_task >> subtraction_task
subtraction_task >> division_task
addition_task >> division_task



