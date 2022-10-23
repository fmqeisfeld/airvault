from pathlib import Path
from urllib.parse import scheme_chars
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


from airflow.operators.python_operator import PythonOperator

from operators.hub_stager import Hub_Stager
from operators.link_stager import Link_Stager
from operators.sat_stager import Sat_Stager


from operators.hub_loader import Hub_Loader 
from operators.link_loader import Link_Loader
from operators.sat_loader import Sat_Loader

from collections import defaultdict

DAG_ID = "extract"

DAG_DIR = Path(__file__).parent
CONF_DIR = "configs"
CONF_FILE='conf.yaml'
CONF_FILE_PATH = DAG_DIR / CONF_DIR / CONF_FILE
config_file_path = Path(CONF_FILE_PATH)

# aux. function to convert conf-dict to defaultdict
def defaultify(d):
    if isinstance(d, dict):
        return defaultdict(lambda: None, {k: defaultify(v) for k, v in d.items()})
    elif isinstance(d, list):
        return [defaultify(e) for e in d]
    else:
        return d   


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
        #print(conf)
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
    # edwh tables
    hook = PostgresHook(postgres_conn_id='pgconn')                                      
    sql = 'select table_name from information_schema."tables" where table_schema = \'{tbl}\';'.format(tbl=conns['schemas']['edwh'])
    result=hook.get_records(sql)        
    tbl_list= [tbl[0] for tbl in result] #list of tuples (e.g. [(h_customer,), (h_store,)) to list of items
    Variable.set('vault_tables',tbl_list)            
      

@task(task_id='set_vars')
def set_vars(conf:dict):
    """ Sets global vars"""
    SCHEMA_SOURCE = conf['connection']['schemas']['source']
    SCHEMA_STAGE = conf['connection']['schemas']['stage']
    SCHEMA_EDWH = conf['connection']['schemas']['edwh']          

    Variable.set('SCHEMA_STAGE',SCHEMA_STAGE)
    Variable.set('SCHEMA_EDWH',SCHEMA_EDWH)
    Variable.set('SCHEMA_SOURCE',SCHEMA_SOURCE)
    Variable.set('MAXVARCHAR',256)

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
    #if config_file_path.exists():
    #    with open(config_file_path, "r") as config_file:
    #        config = yaml.safe_load(config_file)  
    read_config=DummyOperator(task_id='read_config')          
    config_file = open(config_file_path,'r').read()
    config = yaml.safe_load(config_file)        
    config = defaultify(config)
            

    # warning: don't do the following. Dag get's stuck - Tasks won't start
    #Variable.set('SCHEMA_STAGE',SCHEMA_STAGE)
    #Variable.set('SCHEMA_EDWH',SCHEMA_EDWH)
    # Reason: Put logic outside of dag into separate Task/Operator
    # c.f.: https://www.astronomer.io/blog/7-common-errors-to-check-when-debugging-airflow-dag/


    # ***********************
    #       STAGING
    # ***********************
    with TaskGroup(group_id='STAGING') as staging_group:
        with TaskGroup(group_id='stg_hubs') as stg_hub_group:    
            for i,hub in enumerate(config['rv']['hubs']):
                stage_hub = Hub_Stager(task_id=f"stage_{hub}", 
                                        conf=config,
                                        hub=hub)
                
        with TaskGroup(group_id='stg_links') as stg_lnk_group:    
            for i,lnk in enumerate(config['rv']['links']):
                stage_lnk = Link_Stager(task_id=f"stage_{lnk}", 
                                        conf=config,
                                        lnk=lnk)       
                
        with TaskGroup(group_id='stg_sats') as stg_sat_group:    
            for i,sat in enumerate(config['rv']['sats']):
                stage_sat = Sat_Stager(task_id=f"stage_{sat}", 
                                        conf=config,
                                        sat=sat)                           
    # ***********************
    #       RAW VAULT
    # ***********************
    with TaskGroup(group_id='LOAD_RAW_VAULT') as rv_group:
        # ***********************
        #       RV HUBS
        # ***********************
        with TaskGroup(group_id='load_hubs') as load_hubs:
            for i,hub in enumerate(config['rv']['hubs']):                                
                load_hub = Hub_Loader(task_id=f"load_{hub}", 
                                    conf=config,
                                    hub=hub)                            
                    
        # ***********************
        #       RV LINKS
        # ***********************
        with TaskGroup(group_id='load_links') as load_links:
            for i,lnk in enumerate(config['rv']['links']):                                
                load_link = Link_Loader(task_id=f"load_{lnk}", conf=config,lnk=lnk)
                
        # ***********************
        #       RV SATS
        # ***********************
        with TaskGroup(group_id='load_sats') as load_sats:
            for i,sat in enumerate(config['rv']['sats']):                                
                load_sat = Sat_Loader(task_id=f"load_{sat}", conf=config,sat=sat)                
        
    # read_config >> connect(conf=config['connection']) >> get_tables(conns=config['connection']) >> [hub_group,link_group]

    #read_config >> set_vars(SCHEMA_STAGE=SCHEMA_STAGE,SCHEMA_EDWH=SCHEMA_EDWH) 
    #>> connect(conf=config['connection']) >> get_tables(conns=config['connection']) >> [hub_group]

    # debug                                
    #hub = 'hub_tax_bundle'
    #create_hub = Hub_Creator(task_id=f"create_hub", conf=config,hub=hub)
    
    #read_config >> set_vars(conf=config) >> connect(conf=config['connection']) >> get_tables(conns=config['connection']) >> rv_group #[hub_group,link_group]
    
    read_config >> set_vars(conf=config) >> connect(conf=config['connection']) >> get_tables(conns=config['connection']) >> staging_group >> rv_group



globals()[DAG_ID] = mydag()