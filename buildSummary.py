from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def create_session_summary():
    # Initialize Snowflake Hook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cur = conn.cursor()

    # SQL to create the session_summary table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dev.analytics.session_summary AS 
    SELECT 
        u.userId,
        u.sessionId,
        u.channel,
        s.ts
    FROM 
        dev.raw_data.user_session_channel u
    JOIN 
        dev.raw_data.session_timestamp s 
    ON 
        u.sessionId = s.sessionId
    WHERE 
        s.ts IS NOT NULL
    AND 
        (u.userId, u.sessionId) NOT IN (
            SELECT userId, sessionId FROM dev.analytics.session_summary
        )
    ;
    """

    # Execute the SQL command
    cur.execute(create_table_sql)
    cur.close()
    conn.close()

with DAG(
    dag_id='session_summary_dag',
    start_date=datetime(2024, 10, 2),
    schedule_interval='@daily',
    catchup=False,
    tags=['ELT'],
) as dag:

    create_summary_task = PythonOperator(
        task_id='create_session_summary',
        python_callable=create_session_summary,
    )

    create_summary_task
