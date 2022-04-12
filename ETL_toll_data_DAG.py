# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'abc',
    'start_date': days_ago(0),
    'email': ['abc@abc.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the task 'unzip'

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -C /home/project/airflow/dags/finalassignment/staging -zxvf tolldata.tgz',
    dag=dag,
)

# define the task 'extract'

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f 1,2,3,4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# define the task 'extract2'

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 5,6,7 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)

# define the task 'extract3'

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-61,63-67 --output-delimiter=, payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

# define the task 'consolidate'

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted.csv',
    dag=dag,
)

# define the task 'transform'

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < extracted.csv > transformed_data.csv',
    dag=dag,
)

# task pipeline

unzip_data >> extract_data_from_csv >> 	extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
