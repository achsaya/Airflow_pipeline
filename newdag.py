from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import imp

def load_file_spark():
        spark_file = imp.load_source('spark_file', '/root/airflow/inputfiles/timber_project.py')

default_args = {
            'start_date': datetime(2023, 6, 1),
                'email_on_failure': False,
                    'email_on_success': True,
                        'email': ['achsaya.912@gmail.com']
                        }
# Instantiate a DAG object
project_dag = DAG('project',
                default_args=default_args,
                        description='Airflow_project',
                                schedule_interval='* * * * *',
                                        catchup=False,
                                                tags=['example, project']
                                                )

start_task = DummyOperator(task_id='start_task',dag=project_dag)

spark_job=PythonOperator(
            task_id='spark_task',
                python_callable=load_file_spark,
                    dag=project_dag
                    )


end_task= DummyOperator(task_id='end_task',dag=project_dag)
email_notification = EmailOperator(
                task_id="success_notificatio",
                        to=['achsaya.912@gmail.com'],
                                subject="Alert Airflow Notification",
                                        html_content='<p>The task ran successfully.</p>',
                                                trigger_rule='all_success',
                                                        cc=['achsaya.912@gmail.com'],
                                                                dag=project_dag
                                                                    )
start_task >> spark_job >> end_task >> email_notification