from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

DEFAULT_ARGS = {
    'owner': 'vishal',
    'depends_on_past': False,
    'start_date': days_ago(0),
}

dag = DAG('sampleETL', default_args=DEFAULT_ARGS, schedule_interval=None)

def function_downloading(filename):
    print(filename)
    return filename

def function_extracting(df):
    print(df)
    return df

def function_upload(df,name):
    print(df)
    print(name)
    return df


downloading = PythonOperator(
    task_id='downloading',
    python_callable=function_downloading,
    op_kwargs = {"filename" : "test.csv"},
    dag=dag,
)

extracting = PythonOperator(
    task_id='extracting',
    python_callable=function_extracting,
    op_kwargs = {"df" : "pandas_df"},
    dag=dag,
)

upload = PythonOperator(
    task_id='upload',
    python_callable=function_upload,
    op_kwargs = {"df" : "pandas_df","name":"test.csv"}, 
    # op_kwargs = {"df" : "pandas_df"},
    dag=dag,
)

downloading >> extracting >> upload