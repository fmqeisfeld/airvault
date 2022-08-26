from pathlib import Path
import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook

from airflow.models import Connection
from airflow import settings

from airflow.exceptions import AirflowNotFoundException

DAG_ID = "extract"

DAG_DIR = Path(__file__).parent
CONF_DIR = "configs"
CONF_FILE='conf.yaml'
CONF_FILE_PATH = DAG_DIR / CONF_DIR / CONF_FILE
config_file_path = Path(CONF_FILE_PATH)



@task(task_id='connect')
def connect(conf):
    """ 
        connects to dbs
    """

    #print(conf)
    session = settings.Session()
    
    try:
        stage_conn=BaseHook.get_connection('stage_conn')
    except AirflowNotFoundException:
        stage_conn = Connection(conn_id='stage_conn',
                                login=conf['stage']['user'],
                                host=conf['stage']['host'],
                                port=conf['stage']['port'],
                                password=conf['stage']['pw'],
                                conn_type='postgres'
                                )
        session.add(stage_conn)
        session.commit()

    try:
        edwh_conn=BaseHook.get_connection('edwh_conn')
    except AirflowNotFoundException:         
        edwh_conn = Connection(conn_id='edwh_conn',
                                login=conf['edwh']['user'],
                                host=conf['edwh']['host'],
                                port=conf['edwh']['port'],
                                password=conf['edwh']['pw'],
                                conn_type='postgres'
                                )                            
    
        session.add(edwh_conn)
        session.commit()
    
    stage_conn.test_connection()
    edwh_conn.test_connection()
    
    

@dag(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None, catchup=False
)
def mydag():
    """
    Mydag docstring
    """
    read_config = DummyOperator(task_id="read_config")

    if config_file_path.exists():
        with open(config_file_path, "r") as config_file:
            config = yaml.safe_load(config_file)

            read_config >> connect(conf=config['connections'])


globals()[DAG_ID] = mydag()