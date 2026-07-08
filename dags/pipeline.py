import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from generate_report import generate_gold_report

# =============================================================================
# 1. Configuration & Default Arguments
# =============================================================================
# Directory where your Parquet files land (update this to your local/server path)
LANDING_ZONE_PATH = "/opt/airflow/data/landing"

default_args = {
    'owner': 'data_engineering_admin',
    'depends_on_past': False, # Allows independent monthly runs
    'start_date': datetime(2026, 4, 1), # Starts executing for April 2026
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Helper function to load Parquet to PostgreSQL Bronze tables
def load_parquet_to_postgres(file_path, table_name, **kwargs):
    """
    Reads a Parquet file using Polars and bulk-inserts to PostgreSQL.
    """
    import polars as pl
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    print(f"Executing dynamic ingestion for file: {file_path}")
    print(f"Target table: {table_name}")
    
    # Get connection URI from Airflow's PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    uri = hook.get_uri()
    
    # Read parquet using Polars
    df = pl.read_parquet(file_path)
    
    # Write to postgres
    df.write_database(
        table_name=f"bronze.{table_name}",
        connection=uri,
        if_table_exists='append',
        engine='adbc'
    )
    print("Ingestion complete.")

def load_csv_to_postgres(file_path, table_name, **kwargs):
    """
    Reads a CSV file using Polars and bulk-inserts to PostgreSQL.
    """
    import polars as pl
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    print(f"Executing static ingestion for file: {file_path}")
    print(f"Target table: {table_name}")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    uri = hook.get_uri()
    
    # Read CSV using Polars
    df = pl.read_csv(file_path)
    
    # Write to postgres
    df.write_database(
        table_name=f"bronze.{table_name}",
        connection=uri,
        if_table_exists='replace',
        engine='adbc'
    )
    print("Ingestion complete.")

# =============================================================================
# 2. DAG Definition
# =============================================================================
with DAG(
    dag_id='dynamic_nyc_taxi_medallion',
    default_args=default_args,
    description='Automated monthly ELT pipeline for Yellow and Green Taxis',
    schedule_interval='@monthly', # Triggers automatically at the end of each month
    catchup=False, # Set to True if you want to automatically process historical months
    template_searchpath=['/opt/airflow/dags/sql/'], # Path to your SQL scripts
    tags=['portfolio', 'medallion', 'taxi'],
) as dag:

    # =========================================================================
    # INITIALIZATION: Set up schemas and tables
    # =========================================================================
    with TaskGroup('initialize_schemas') as init_schemas:
        init_bronze = SQLExecuteQueryOperator(
            task_id='init_bronze',
            sql='init_bronze_schema.sql',
            conn_id='postgres_default'
        )
        init_silver = SQLExecuteQueryOperator(
            task_id='init_silver',
            sql='init_silver_schema.sql',
            conn_id='postgres_default'
        )
        init_gold = SQLExecuteQueryOperator(
            task_id='init_gold',
            sql='init_gold_schema.sql',
            conn_id='postgres_default'
        )

    # =========================================================================
    # PARALLEL STREAM 0: Static Data Processing
    # =========================================================================
    with TaskGroup('process_static_data') as static_stream:
        load_bronze_zones = PythonOperator(
            task_id='load_bronze_zones',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'file_path': f"{LANDING_ZONE_PATH}/taxi_zone_lookup.csv",
                'table_name': 'zone_lookup'
            }
        )

    # =========================================================================
    # PARALLEL STREAM 1: Yellow Taxi Processing
    # =========================================================================
    with TaskGroup('process_yellow_taxis') as yellow_stream:
        
        # Dynamic File Sensor using Jinja templating
        sense_yellow_file = FileSensor(
            task_id='sense_yellow_parquet',
            filepath=f"{LANDING_ZONE_PATH}/yellow_tripdata_{{{{ data_interval_start.strftime('%Y-%m') }}}}.parquet",
            fs_conn_id='fs_default',
            poke_interval=60, # Check every 60 seconds
            timeout=3600,     # Fail after 1 hour if file doesn't arrive
            mode='poke'
        )

        load_bronze_yellow = PythonOperator(
            task_id='load_bronze_yellow',
            python_callable=load_parquet_to_postgres,
            op_kwargs={
                'file_path': f"{LANDING_ZONE_PATH}/yellow_tripdata_{{{{ data_interval_start.strftime('%Y-%m') }}}}.parquet",
                'table_name': 'yellow_trips'
            }
        )

        dq_check_yellow = SQLExecuteQueryOperator(
            task_id='dq_check_yellow',
            sql="""
                SELECT COUNT(*) FROM bronze.yellow_trips 
                WHERE trip_distance < 0 OR total_amount < 0;
            """,
            conn_id='postgres_default'
        )

        sense_yellow_file >> load_bronze_yellow >> dq_check_yellow

    # =========================================================================
    # PARALLEL STREAM 2: Green Taxi Processing
    # =========================================================================
    with TaskGroup('process_green_taxis') as green_stream:
        
        # Dynamic File Sensor using Jinja templating
        sense_green_file = FileSensor(
            task_id='sense_green_parquet',
            filepath=f"{LANDING_ZONE_PATH}/green_tripdata_{{{{ data_interval_start.strftime('%Y-%m') }}}}.parquet",
            fs_conn_id='fs_default',
            poke_interval=60,
            timeout=3600,
            mode='poke'
        )

        load_bronze_green = PythonOperator(
            task_id='load_bronze_green',
            python_callable=load_parquet_to_postgres,
            op_kwargs={
                'file_path': f"{LANDING_ZONE_PATH}/green_tripdata_{{{{ data_interval_start.strftime('%Y-%m') }}}}.parquet",
                'table_name': 'green_trips'
            }
        )

        dq_check_green = SQLExecuteQueryOperator(
            task_id='dq_check_green',
            sql="""
                SELECT COUNT(*) FROM bronze.green_trips 
                WHERE trip_distance < 0 OR total_amount < 0;
            """,
            conn_id='postgres_default'
        )

        sense_green_file >> load_bronze_green >> dq_check_green

    # =========================================================================
    # CONVERGENCE: Silver Transformation & Gold Analytics (SQL Execution)
    # =========================================================================
    
    # Executes external SQL files to keep the DAG clean
    transform_silver_layer = SQLExecuteQueryOperator(
        task_id='transform_silver_layer',
        sql='silver_transformations.sql',
        conn_id='postgres_default'
    )

    generate_gold_analytics = SQLExecuteQueryOperator(
        task_id='generate_gold_analytics',
        sql='gold_aggregations.sql',
        conn_id='postgres_default'
    )

    # =========================================================================
    # FINAL NODE: Generate Interactive HTML Dashboard
    # =========================================================================
    generate_html_report = PythonOperator(
        task_id='generate_html_report',
        python_callable=generate_gold_report,
    )

    # Dependencies: Initialization must finish before loading streams
    init_schemas >> [yellow_stream, green_stream, static_stream]
    
    # Dependencies: All streams must succeed before moving to the Silver layer
    [yellow_stream, green_stream, static_stream] >> transform_silver_layer >> generate_gold_analytics >> generate_html_report