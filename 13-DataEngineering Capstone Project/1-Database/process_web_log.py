# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#define the DAG arguments
default_args = {
    'owner': 'Mohammed Abdullah',
    'start_date': days_ago(0),
    'email': ['xmxd10@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# Define the DAG with the parameters
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='web process log',
    schedule_interval=timedelta(days=1),
)

# the first task is to extract the ipaddress field from web log
extract_data = BashOperator(
    task_id="extract_data",
    bash_command='cut -d "" -f1 /home/project/airflow/dags/capstone/accesslog.txt \
    > /home/project/airflow/dags/capstone/extracted_data.txt ',
    dag=dag,
)

# the second task is to filter out all the occurrences of ipaddress
transform_data = BashOperator(
    task_id="transform_data",
    bash_command='sed "/198.46.149.143/d" /home/project/airflow/dags/capstone/extracted_data.txt \
    > /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)

# the third task should archive the file transform_data.txt into weblog.tar
load_data = BashOperator(
    task_id="load_data",
    bash_command='tar cvf weblog /home/project/airflow/dags/capstone/transformed_data.txt ',
    dag=dag,
)

# Pipeline
extract_data >> transform_data >> load_data