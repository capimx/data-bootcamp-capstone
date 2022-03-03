from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from custom_modules.operator_s3_to_postgres import S3ToPostgresTransfer
from airflow.operators.python_operator import PythonOperator

create_table_schema_cmd = """
        CREATE SCHEMA purchase;
        CREATE TABLE IF NOT EXISTS purchase.purchases (    
                        invoice_number varchar(10),
                        stock_code     varchar(20),
                        detail         varchar(1000),
                        quantity       integer,
                        invoice_date   timestamp,
                        unit_price     numeric(8,3),
                        customer_id    integer,
                        country        varchar(20));
          """

def print_welcome():
    return 'Welcome from custom operator - Airflow DAG!'

dag = DAG('dag_insert_data', 
          description='Inser Data from CSV To Postgres',
          schedule_interval='@once',        
          start_date=datetime(2021, 10, 1),
          catchup=False)

welcome_operator = PythonOperator(task_id='welcome_task', 
                                  python_callable=print_welcome, 
                                  dag=dag)

create_table_operator = PostgresOperator(   task_id="create_table_task",
                                            sql=create_table_schema_cmd,
                                            aws_conn_postgres_id = 'postgres_default',
                                            dag=dag
                                        )

s3_to_postgres_operator = S3ToPostgresTransfer(
                            task_id = 'dag_s3_to_postgres',
                            schema =  'purchase', #'public'
                            table= 'purchases',
                            s3_bucket = 'wz-de-academy-mau-raw-data-bucket',
                            s3_key =  'user_purchase.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default',   
                            dag = dag
)

welcome_operator >> create_table_operator >> s3_to_postgres_operator

#welcome_operator.set_downstream(s3_to_postgres_operator)
