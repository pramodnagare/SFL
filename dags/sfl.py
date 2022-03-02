from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd

from sfl.config import meta_config

airflow_var = Variable.get('sfl_var', deserialize_json=True)
internal_email = airflow_var.get('internal_email')  # {"internal_email": "pramodnagare1993@gmail.com"}
test_date = (datetime.now() + timedelta(1)).date()

create_table_sql = open(meta_config.create_table_sql, 'r').read()
delete_rows_sql = open(meta_config.delete_rows_sql, 'r').read()
delete_xcom_data = open(meta_config.delete_xcom_sql, 'r').read()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': internal_email,
    'email_on_failure': True,
    'email_on_retry': False,
    'provide_context': True
}

AIRFLOW_DAG_ID = 'sfl_dag'
AIRFLOW_DAG_DESC = 'SFL Data Challenge solution'

dag = DAG(
    dag_id=AIRFLOW_DAG_ID,
    default_args=default_args,
    description=AIRFLOW_DAG_DESC,
    schedule_interval='00 9 * * *',
    max_active_runs=1,
    concurrency=20,
    catchup=False
)


def read_data(file_path):
    data = pd.read_csv(file_path)
    return data


def transform_data(data):
    data = data.fillna({'first_name': 'FNU',
                        'last_name': 'LNU',
                        'email': '',
                        'gender': '',
                        'ip_address': ''})

    data['domain'] = data['email'].apply(lambda x: x.split('@')[-1])

    return data


def validate_data(**kwargs):

    data = read_data(kwargs["filepath"])
    if sorted(kwargs["column_list"]) != sorted(data.columns):
        return "schema_error"

    return "create_sfl_table"


def load_data(**kwargs):

    data = read_data(kwargs["filepath"])
    if sorted(kwargs["column_list"]) != sorted(data.columns):
        return "schema_error"

    data = transform_data(data)
    engine = create_engine(kwargs["database_url"])

    data.to_sql(kwargs["tablename"], engine, if_exists="append", index=False)

    return "clear_xcom"


load_data = BranchPythonOperator(
    task_id="load_sfl_data",
    python_callable=load_data,
    op_kwargs={
        "filepath": meta_config.sfl_data_filepath,
        "tablename": meta_config.sfl_table_name,
        "column_list": meta_config.sfl_data_column_list,
        "database_url": meta_config.sfl_database_url
    },
    retries=1,
    retry_delay=5,
    provide_context=True,
    trigger_rule="one_success",
    dag=dag
)


validate_data = BranchPythonOperator(
    task_id="validate_sfl_data",
    python_callable=validate_data,
    op_kwargs={
        "filepath": meta_config.sfl_data_filepath,
        "column_list": meta_config.sfl_data_column_list
    },
    retries=1,
    retry_delay=5,
    provide_context=True,
    trigger_rule="one_success",
    dag=dag
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

create_table = PostgresOperator(
    task_id="create_sfl_table",
    sql=create_table_sql.format(meta_config.sfl_table_name),
    trigger_rule="one_success",
    dag=dag
)

delete_rows = PostgresOperator(
    task_id="delete_old_sfl_data",
    sql=delete_rows_sql.format(meta_config.sfl_table_name, test_date),
    trigger_rule="one_success",
    dag=dag
)

schema_error = DummyOperator(
    task_id='schema_error',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

clear_xcom = PostgresOperator(
    task_id='clear_xcom',
    postgres_conn_id='airflow_db',
    trigger_rule='all_done',
    sql=delete_xcom_data.format(AIRFLOW_DAG_ID),
    dag=dag
)

start >> validate_data >> create_table >> delete_rows >> load_data >> clear_xcom >> end
validate_data >> schema_error >> clear_xcom >> end
