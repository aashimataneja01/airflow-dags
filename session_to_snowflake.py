from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Function to return a Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_tables():
    cur = return_snowflake_conn()
    try:
        # Create user_session_channel table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
                userId int NOT NULL,
                sessionId varchar(32) PRIMARY KEY,
                channel varchar(32) DEFAULT 'direct'
            )
        """)
        
        # Create session_timestamp table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
                sessionId varchar(32) PRIMARY KEY,
                ts timestamp
            )
        """)
    except Exception as e:
        print(e)
        raise e

@task
def copy_user_session_channel():
    cur = return_snowflake_conn()
    try:
        cur.execute("""
            COPY INTO dev.raw_data.user_session_channel
            FROM @dev.raw_data.blob_stage/user_session_channel.csv
            FILE_FORMAT = (TYPE = 'csv', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        """)
    except Exception as e:
        print(e)
        raise e

@task
def copy_session_timestamp():
    cur = return_snowflake_conn()
    try:
        cur.execute("""
            COPY INTO dev.raw_data.session_timestamp
            FROM @dev.raw_data.blob_stage/session_timestamp.csv
            FILE_FORMAT = (TYPE = 'csv', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        """)
    except Exception as e:
        print(e)
        raise e

# Define the DAG
with DAG(
    dag_id='SessionToSnowflake',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ETL'],
    schedule='15 1 * * *',  # Adjust the schedule as needed
) as dag:
    
    # Define the task sequence
    create_tables_task = create_tables()
    copy_user_session_channel_task = copy_user_session_channel()
    copy_session_timestamp_task = copy_session_timestamp()

    create_tables_task >> [copy_user_session_channel_task, copy_session_timestamp_task]
