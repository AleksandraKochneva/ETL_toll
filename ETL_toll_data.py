from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Constants for file paths
BASE_PATH = '/Users/aleksandrakochneva/Desktop/Portfolio/ETL_toll'
TAR_FILE = f'{BASE_PATH}/tolldata.tgz'
EXTRACTED_FOLDER = f'{BASE_PATH}/tolldata'
CSV_FILE = f'{EXTRACTED_FOLDER}/vehicle-data.csv'
TSV_FILE = f'{EXTRACTED_FOLDER}/tollplaza-data.tsv'
FIXED_WIDTH_FILE = f'{EXTRACTED_FOLDER}/payment-data.txt'
OUTPUT_FOLDER = BASE_PATH
EXTRACTED_CSV = f'{OUTPUT_FOLDER}/csv_data.csv'
EXTRACTED_TSV = f'{OUTPUT_FOLDER}/tsv_data.csv'
EXTRACTED_FIXED_WIDTH = f'{OUTPUT_FOLDER}/fixed_width_data.csv'
CONSOLIDATED_DATA = f'{OUTPUT_FOLDER}/extracted_data.csv'
TRANSFORMED_DATA = f'{OUTPUT_FOLDER}/transformed_data.csv'

# Default args for the DAG
default_args = {
    'owner': 'Aleksandra Kochneva',
    'start_date': days_ago(0),
    'email': ['aleksakochneva2022@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Task: Unzip the data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xvf {TAR_FILE} -C {EXTRACTED_FOLDER}',
    dag=dag,
)

# Task: Extract data from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f'cut -d"," -f1-4 {CSV_FILE} > {EXTRACTED_CSV}',
    dag=dag,
)

# Task: Extract data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f'cut -d"\t" -f5-7 {TSV_FILE} > {EXTRACTED_TSV}',
    dag=dag,
)

# Task: Extract data from fixed-width format
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f'cut -c56-65,66-70 {FIXED_WIDTH_FILE} > {EXTRACTED_FIXED_WIDTH}',
    dag=dag,
)

# Task: Consolidate extracted data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f'paste -d, {EXTRACTED_CSV} {EXTRACTED_TSV} {EXTRACTED_FIXED_WIDTH} > {CONSOLIDATED_DATA}',
    dag=dag,
)

# Task: Transform the consolidated data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'''awk -F, 'BEGIN{{OFS=","}} {{$4=toupper($4)}}1' {CONSOLIDATED_DATA} > {TRANSFORMED_DATA}''',
    dag=dag,
)

# Task dependencies
unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width]
[extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data
consolidate_data >> transform_data
