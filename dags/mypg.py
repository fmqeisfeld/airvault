from pathlib import Path
import pendulum
import yaml
from airflow.decorators import dag, task
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Connection
from airflow import settings
from airflow.models import Variable
from airflow.exceptions import AirflowNotFoundException


from operators.hub_loader import Hub_Loader 
from operators.hub_creator import Hub_Creator
from operators.link_creator import Link_Creator
from operators.link_loader import Link_Loader

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
        pgconn=BaseHook.get_connection('pgconn')
    except AirflowNotFoundException:
        print(conf)
        pgconn = Connection(conn_id='pgconn',
                                login=conf['user'],
                                host=conf['host'],
                                port=conf['port'],
                                password=conf['pw'],
                                schema=conf['db'],                                
                                conn_type='postgres'
                                )
        session.add(pgconn)
        session.commit()
        

@task(task_id='get_tables')    
def get_tables(conns):   
    """
    queries the information schema to retrieve the available tables and saves the list as global var
    """     
    #print(conf)
    hook = PostgresHook(postgres_conn_id='pgconn')                                      
    sql = 'select table_name from information_schema."tables" where table_schema = \'{tbl}\';'.format(tbl=conns['schemas']['edwh'])
    result=hook.get_records(sql)        
    tbl_list= [tbl[0] for tbl in result] #list of tuples (e.g. [(h_customer,), (h_store,)) to list of items
    Variable.set('vault_tables',tbl_list)            

@dag(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None, 
    catchup=False,
    template_searchpath="/opt/airflow/dags/sql"
)
def mydag():
    """
    Mydag docstring
    """
    read_config = DummyOperator(task_id="read_config")

    if config_file_path.exists():
        with open(config_file_path, "r") as config_file:
            config = yaml.safe_load(config_file)

    # ***********************
    #       HUBS
    # ***********************
    with TaskGroup(group_id='hubs') as hub_group:
        # create hubs if not already present
        with TaskGroup(group_id='create_hubs') as create_hubs:
            for i,hub in enumerate(config['hubs']):
                create_hub = Hub_Creator(task_id=f"create_{hub}", conf=config,hub=hub)
                #create_hub = DummyOperator(task_id=f"create_{hub}")

        # Hub loader-routine using template sql
        with TaskGroup(group_id='load_hubs') as load_hubs:
            for i,hub in enumerate(config['hubs']):                                
                load_hub = Hub_Loader(task_id=f"load_{hub}", conf=config,hub=hub)
                #load_hub = DummyOperator(task_id=f"load_{hub}")
    
        create_hubs >> load_hubs
                
    # ***********************
    #       LINKS
    # ***********************
    with TaskGroup(group_id='links') as link_group:
        # create links if not already present
        with TaskGroup(group_id='create_links') as create_links:
            for i,link in enumerate(config['links']):
                #create_hub = Hub_Creator(task_id=f"create_{hub}", conf=config,hub=hub)
                create_link = Link_Creator(task_id=f"create_{link}", conf=config,link=link)

        # Link loader-routine using template sql
        with TaskGroup(group_id='load_links') as load_links:
            for i,link in enumerate(config['links']):                                
                load_link = Link_Loader(task_id=f"load_{link}", conf=config,link=link)
                #load_link = DummyOperator(task_id=f"load_{link}")
    
        create_links >> load_links
    read_config >> connect(conf=config['connection']) >> get_tables(conns=config['connection']) >> [hub_group,link_group]


    # debug                                
    #hub = 'hub_tax_bundle'
    #create_hub = Hub_Creator(task_id=f"create_hub", conf=config,hub=hub)
    #read_config >> connect(conf=config['connection']) >> get_tables(conns=config['connection']) >> create_hub    



globals()[DAG_ID] = mydag()