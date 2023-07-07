from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
import time

def send_task_email(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    subject = f'Task {task_id} in DAG {dag_id} has started'
    html_content = '<p>The task has started.</p>'
    send_email(to=['your_email@example.com'], subject=subject, html_content=html_content)


AIRFLOW_HOME = "/home/shashank/Documents/ABC/elt"
cmd_ingestion = f"python {AIRFLOW_HOME}/run_app.py 2 {AIRFLOW_HOME}/elt_cli/config/default.yml"
cmd_landing = f"python {AIRFLOW_HOME}/run_app.py 3 /{AIRFLOW_HOME}/elt_cli/config/default.yml"
cmd_curation = f"python {AIRFLOW_HOME}/run_app.py 4 {AIRFLOW_HOME}/elt_cli/config/default.yml"
cmd_consumption = f"python {AIRFLOW_HOME}/run_app.py 5 {AIRFLOW_HOME}/elt_cli/config/default.yml"


default_args = {
    'owner':'shashank',
    'retries':1,
    'retry_delay':timedelta(minutes=2),
    'email':["snowflake29091999@gmail.com"],
    'email_on_failure':True,
    'email_on_retry':True
}

def sleep_fun():
    time.sleep(10)

with DAG(
    dag_id="ETL_SETUP",
    description="this dag is used to setup etl",
    default_args=default_args,
    start_date=datetime(2023,3,17,2),
    schedule_interval="1 1 1 * *"
) as dag:
    start_mail = EmailOperator(
        task_id='start_mail',
        to='snowflake29091999@gmail.com',
        subject='Schema setup started',
        html_content="""<h3> Setup of landing,curation,consumption and ingestion started<h3>
                        <p> Please monitor <p> """
    )

    task0 = BashOperator(
        task_id='start_task',
        bash_command='echo Setup processe started'
    )
    task1 = BashOperator(
        task_id='landing_zone_setup',
        bash_command=cmd_landing,
        email_on_failure=True)
    
    task2 = BashOperator(
        task_id='curated_zone_setup',
        bash_command=cmd_curation,
        email_on_failure=True)
    
    task3 = BashOperator(
        task_id='consumption_zone_setup',
        bash_command=cmd_consumption,
        email_on_failure=True)
    
    task4 = BashOperator(
        task_id='ingestion_setup',
        bash_command=cmd_ingestion,
        email_on_failure=True)
    
    sleep_task = PythonOperator(
        task_id='sleep_task',
        python_callable=sleep_fun)
    
    finish_mail = EmailOperator(
        task_id='finish_mail',
        to='snowflake29091999@gmail.com',
        subject='Schema setup Finished Succefully',
        html_content="""<h3> Setup of landing,curation,consumption and ingestion Finished<h3>
                        <p> Please verify <p> """
    )
        
    start_mail >> task0 >> [task1,task2,task3,task4] >> sleep_task >> finish_mail

    # start_mail.set_downstream(task0)
    # task0.set_downstream([task1, task2, task3, task4])
    # task1.set_downstream(sleep_task)
    # task2.set_downstream(sleep_task)
    # task3.set_downstream(sleep_task)
    # task4.set_downstream(sleep_task)
    # sleep_task.set_downstream(finish_mail)

