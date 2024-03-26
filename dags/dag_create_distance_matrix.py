"""
This DAG orchestrates a series of tasks to create a distance matrix.
The tasks involve downloading a database, extracting data from the downloaded database, creating two base datasets ('base_a' and 'base_b'), and finally creating a distance matrix.

Tasks:
1. download_database: Downloads the required database.
2. database_extracted: Extracts necessary data from the downloaded database.
3. create_base_a: Creates base dataset 'base_a'.
4. create_base_b: Creates base dataset 'base_b'.
5. create_distance_matrix_task: Creates a distance matrix using the created base datasets.

Dependencies:
- 'download_database' must be completed before 'database_extracted'.
- 'database_extracted' must be completed before 'create_base_a'.
- 'create_base_a' must be completed before 'create_base_b'.
- 'create_base_b' must be completed before 'create_distance_matrix_task'.

"""

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from download.download_database import download_database
from processing.database_extracted import database_extracted
from processing.create_base_a import create_base_a
from processing.create_base_b import create_base_b
from processing.create_distance_matrix import create_distance_matrix_

def call_download_database():
    """
    Calls the function to download the database.
    """
    download_database()

def call_database_extracted():
    """
    Calls the function to extract data from the downloaded database.
    """
    database_extracted()

def call_create_base_a():
    """
    Calls the function to create base dataset 'base_a'.
    """
    create_base_a()

def call_create_base_b():
    """
    Calls the function to create base dataset 'base_b'.
    """
    create_base_b()

def call_create_distance_matrix():
    """
    Calls the function to create the distance matrix.
    """
    create_distance_matrix_()

# Defining the DAG
with DAG(
    'create_distance_matrix',
    start_date=days_ago(1),
    schedule_interval='@daily',    
) as dag:

    # Task to download the database
    task_download_database = PythonOperator(
        task_id='download_database',
        python_callable=call_download_database,
        dag=dag,
    )

    # Task to extract data from the downloaded database
    task_database_extracted = PythonOperator(
        task_id='database_extracted',
        python_callable=call_database_extracted,
        dag=dag,
    )

    # Task to create base dataset 'base_a'
    task_create_base_a = PythonOperator(
        task_id='create_base_a',
        python_callable=call_create_base_a,
        dag=dag,
    )

    # Task to create base dataset 'base_b'
    task_create_base_b = PythonOperator(
        task_id='create_base_b',
        python_callable=call_create_base_b,
        dag=dag,
    )

    # Task to create the distance matrix
    task_create_distance_matrix= PythonOperator(
        task_id='create_distance_matrix_task',
        python_callable=call_create_distance_matrix,
        dag=dag,
    )

# Defining dependencies between tasks
task_download_database >> task_database_extracted >> task_create_base_a >> task_create_base_b >> task_create_distance_matrix
