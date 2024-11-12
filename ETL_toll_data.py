from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Aleksandra Kochneva',
    'start_date': days_ago(0),
    'email': ['aleksakochneva2022@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)


unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata.tgz -C /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/vehicle-data.csv > /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/csv_data.csv',
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d"\t" -f5-7 /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/tollplaza-data.tsv > /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/tsv_data.csv',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c56-65,66-70 /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/payment-data.txt > /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d, /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/csv_data.csv /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/tsv_data.csv /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/tolldata/fixed_width_data.csv > /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='''awk -F, 'BEGIN{OFS=","} {$4=toupper($4)}1' /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/extracted_data.csv > /Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll/transformed_data.csv''',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data