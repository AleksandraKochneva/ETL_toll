# Creating ETL Data Pipelines using Apache Airflow

This project is part of the **final project from the IBM ETL and Data Pipelines with Shell, Airflow, and Kafka** course. The goal of this project is to **collect and consolidate road traffic data** from different toll plazas into a single file for analysis. Since each highway is operated by a different toll operator with varying IT setups, the data comes in different file formats. This ETL pipeline automates the extraction, transformation, and loading of the toll data to enable traffic analysis that helps de-congest national highways.

## Project Structure

- **`ETL_toll_data.py`**: The primary Directed Acyclic Graph (DAG) file for Apache Airflow that defines all the ETL tasks.
- **`requirements.txt`**: A list of required Python packages to run this project.

## Project Overview

The ETL pipeline in `ETL_toll_data.py` performs the following steps:

1. **Unzip Data**: Extracts files from a compressed `.tgz` file (`tolldata.tgz`).
2. **Extract Data**: Extracts relevant data from CSV, TSV, and fixed-width files.
3. **Consolidate Data**: Merges the extracted data into a single consolidated file.
4. **Transform Data**: Converts the fourth field in the consolidated data to uppercase for consistency.

## Pipeline DAG Structure

- **DAG Name**: `ETL_toll_data`
- **Schedule**: Runs daily (`timedelta(days=1)`)
- **Owner**: Aleksandra Kochneva
- **Retry Policy**: 1 retry with a delay of 5 minutes.

## ETL Tasks

The ETL pipeline includes the following tasks:

1. **Unzip Data**: Extracts the `tolldata.tgz` file to a specified directory.
2. **Extract Data from CSV**: Extracts the first four columns from `vehicle-data.csv`.
3. **Extract Data from TSV**: Extracts columns 5-7 from `tollplaza-data.tsv`.
4. **Extract Data from Fixed-Width File**: Extracts payment-related data from `payment-data.txt`.
5. **Consolidate Data**: Merges all the extracted data into a single file (`extracted_data.csv`).
6. **Transform Data**: Applies transformations (e.g., converting the fourth column to uppercase) and saves the result to `transformed_data.csv`.

## File Locations

The data files are located in the following directories:

- **Source Data**:
  - `vehicle-data.csv`
  - `tollplaza-data.tsv`
  - `payment-data.txt`
- **Output Data**:
  - `csv_data.csv`
  - `tsv_data.csv`
  - `fixed_width_data.csv`
  - `extracted_data.csv`
  - `transformed_data.csv`

## Prerequisites

- **Apache Airflow**
- **Python 3.12**
- Required Python packages are listed in `requirements.txt`.

### Installing Dependencies

To install the required dependencies, run the following command:
pip install -r requirements.txt

## Usage

1. Clone the repository:
git clone
2. Navigate to the project directory:
cd ETL_toll
3. Run the Apache Airflow web server and scheduler (ensure Airflow is configured):
airflow webserver -p 8080 airflow scheduler
4. Trigger the `ETL_toll_data` DAG in the Airflow UI to start the ETL process.

## Author

- **Aleksandra Kochneva**
- Email: aleksakochneva2022@gmail.com
