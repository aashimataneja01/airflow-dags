from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def run_ctas():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cur = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dev.analytics.session_summary (
        userId int,
        sessionId varchar(32),
        channel varchar(32),
        ts timestamp
    );
    """

    insert_sql = """
    INSERT INTO dev.analytics.session_summary (userId, sessionId, channel, ts)
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
        );
    """

    try:
        # Create the table
        cur.execute(create_table_sql)
        # Insert data
        cur.execute(insert_sql)
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id='session_summary_dag',
    start_date=datetime(2024, 10, 2),
    schedule='45 2 * * *',
    catchup=False,
    tags=['ELT'],
) as dag:

    create_summary_task = PythonOperator(
        task_id='run_ctas',
        python_callable=run_ctas,
    )

    create_summary_task
