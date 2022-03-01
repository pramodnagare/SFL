sfl_data_filepath = "/opt/airflow/dags/data/sfl_data.csv"
sfl_table_name = r"sfl_data"

sfl_data_column_list = ["id", "first_name", "last_name", "email", "gender", "ip_address"]

sfl_database_url = "postgresql://airflow:airflow@postgres/airflow"

ddl_path = "/opt/airflow/dags/sfl/ddl"
sql_path = "/opt/airflow/dags/sfl/sql"

create_table_sql = "{}/create_table.sql".format(ddl_path)
delete_rows_sql = "{}/delete_records.sql".format(sql_path)
delete_xcom_sql = "{}/delete_xcom_data.sql".format(sql_path)
