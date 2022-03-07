from datetime import datetime
import io
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import pandas as pd

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

create_table_schema_cmd = """
        CREATE SCHEMA IF NOT EXISTS purchase;
        CREATE TABLE IF NOT EXISTS purchase.user_purchases (    
                        invoice_number varchar(10),
                        stock_code     varchar(20),
                        detail         varchar(1000),
                        quantity       integer,
                        invoice_date   timestamp,
                        unit_price     numeric(8,3),
                        customer_id    integer,
                        country        varchar(20));
          """

aws_conn_postgres_id = 'postgres_default'

def load_data():
    
    task_id = 'dag_s3_to_postgres'
    schema = 'dbname'
    table= 'user_purchases'
    s3_bucket = 'wz-de-academy-mau-raw-data'
    s3_key =  'user_purchase.csv'    
    aws_conn_id = 'aws_s3_default'

    # Create instances for hooks        
    pg_hook = PostgresHook(postgre_conn_id = aws_conn_postgres_id)
    s3 = S3Hook(aws_conn_id = aws_conn_id, verify = None)

    # Locate file

    if not s3.check_for_key(s3_key, s3_bucket):
        
            logging.error("The key {0} does not exist".format(s3_key))
            
    s3_key_object = s3.get_key(s3_key, s3_bucket)
    
    # # Create table
    # pg_hook.run(create_table_cmd)
    # #curr = pg_hook.get_conn().cursor()

    # 
    file_content = s3_key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
  
    list_target_fields = [    'invoice_number', 
                              'stock_code',
                              'detail', 
                              'quantity', 
                              'invoice_date', 
                              'unit_price', 
                              'customer_id', 
                              'country'
                              ]
    # schema definition for data types of the source.
    schema = {
                'invoice_number': 'string',
                'stock_code': 'string',
                'detail': 'string',
                'quantity': 'string',
                'invoice_date': 'string',
                'unit_price': 'float64',                                
                'customer_id': 'string',
                'country': 'string'
                }  
    date_cols = ['invoice_date']         

    # read a csv file with the properties required.
    df_products = pd.read_csv(io.StringIO(file_content), 
                        header=0, 
                        delimiter=",",
                        quotechar='"',
                        low_memory=False,
                        #parse_dates=date_cols,                                             
                        dtype=schema                         
                        )
    # Reformat df
    df_products = df_products.replace(r"[\"]", r"'")
    df_products['CustomerID'] = df_products['CustomerID'].fillna("")
    df_products['Description'] = df_products['Description'].fillna("")
    list_df_products = df_products.values.tolist()
    list_df_products = [tuple(x) for x in list_df_products]
    current_table = "purchase.user_purchases"

    #Insert rows
    pg_hook.insert_rows(current_table,  
                                list_df_products, 
                                target_fields = list_target_fields, 
                                commit_every = 1000,
                                replace = False) 
   
    print("Finish")   

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

load_data_task = PythonOperator (
    task_id='load_data',
    python_callable=load_data, 
    dag=dag
)

create_table_operator = PostgresOperator(   task_id="create_table_task",
                                            sql=create_table_schema_cmd,
                                            postgres_conn_id = aws_conn_postgres_id,
                                            dag=dag
                                        )

""" s3_to_postgres_operator = S3ToPostgresTransfer(
                            task_id = 'dag_s3_to_postgres',
                            schema =  'purchase', #'public'
                            table= 'purchases',
                            s3_bucket = 'wz-de-academy-mau-raw-data',
                            s3_key =  'user_purchase.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default',   
                            dag = dag
) """

welcome_operator >> create_table_operator >> load_data_task

#welcome_operator.set_downstream(s3_to_postgres_operator)
