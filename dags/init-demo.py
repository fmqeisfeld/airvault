import pendulum
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

with DAG(
    dag_id='init-demo',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None, 
    catchup=False,
) as dag:
       
    create_schemas = PostgresOperator(
        task_id="create_schemas",
        postgres_conn_id="pgconn",
        sql="sql/init_schemas.sql",
    )
    
    load_stage = PostgresOperator(
        task_id="load_demo_stage",
        postgres_conn_id="pgconn",
        sql="sql/demo_stage.sql",
        runtime_parameters={"search_path": "stage"}
    )
    
    create_schemas >> load_stage