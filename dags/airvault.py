from decimal import Context
from multiprocessing import context
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

from operators.bv_hub_stager import BV_Hub_Stager
from operators.bv_sat_stager import BV_Sat_Stager
from operators.bv_link_stager import BV_Link_Stager

from collections import defaultdict

DAG_ID = "airvault"

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
def airvault():
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
            
    task_list={}
    
    # warning: don't do the following. Dag get's stuck - Tasks won't start
    #Variable.set('SCHEMA_STAGE',SCHEMA_STAGE)
    #Variable.set('SCHEMA_EDWH',SCHEMA_EDWH)
    # Reason: Put logic outside of dag into separate Task/Operator
    # c.f.: https://www.astronomer.io/blog/7-common-errors-to-check-when-debugging-airflow-dag/


    # ***********************
    #       STAGING RV
    # ***********************
    with TaskGroup(group_id='STAGING') as staging_group:
        with TaskGroup(group_id='stg_hubs') as stg_hub_group:    
            for i,hub in enumerate(config['rv']['hubs']):
                stage_hub = Hub_Stager(task_id=f"stage_{hub}", 
                                        conf=config['rv'],
                                        hub=hub)                                
                
        with TaskGroup(group_id='stg_links') as stg_lnk_group:    
            for i,lnk in enumerate(config['rv']['links']):
                stage_lnk = Link_Stager(task_id=f"stage_{lnk}", 
                                        conf=config['rv'],
                                        lnk=lnk)       
                
        with TaskGroup(group_id='stg_sats') as stg_sat_group:    
            for i,sat in enumerate(config['rv']['sats']):
                stage_sat = Sat_Stager(task_id=f"stage_{sat}", 
                                        conf=config['rv'],
                                        sat=sat)                           
    # ***********************
    #       LOAD RAW VAULT
    # ***********************
    with TaskGroup(group_id='LOAD_RAW_VAULT') as rv_group:
        # ***********************
        #       RV HUBS
        # ***********************
        with TaskGroup(group_id='load_hubs') as load_hubs:
            for i,hub in enumerate(config['rv']['hubs']):                                
                load_hub = Hub_Loader(task_id=f"load_{hub}", 
                                    conf=config['rv'],
                                    hub=hub)  
                  
                task_list[f'load__{hub}']=load_hub                        
                    
        # ***********************
        #       RV LINKS
        # ***********************
        with TaskGroup(group_id='load_links') as load_links:
            for i,lnk in enumerate(config['rv']['links']):                                
                load_link = Link_Loader(task_id=f"load_{lnk}", conf=config['rv'],lnk=lnk)
                task_list[f'load__{lnk}']=load_link
                
        # ***********************
        #       RV SATS
        # ***********************
        with TaskGroup(group_id='load_sats') as load_sats:
            for i,sat in enumerate(config['rv']['sats']):                                
                load_sat = Sat_Loader(task_id=f"load_{sat}", conf=config['rv'],sat=sat)
                task_list[f'load__{sat}']=load_sat
                
    # ***********************
    #       BV-STAGING
    # ***********************
    dependency_map={}
    with TaskGroup(group_id='BV_STAGING') as bv_staging_group:
        with TaskGroup(group_id='bv_stg_hubs') as bv_stg_hub_group:    
            for i,hub in enumerate(config['bv']['hubs']):
                bv_stage_hub = BV_Hub_Stager(task_id=f"stage_{hub}", 
                                        conf=config['bv'],
                                        hub=hub)
                #bv_stage_hub.set_upstream()
                if config['bv']['hubs'][hub]['dependencies']:
                    dependency_map[bv_stage_hub]=[x for x in config['bv']['hubs'][hub]['dependencies']]
                    
                task_list[f'stage__{hub}']=bv_stage_hub
        
        with TaskGroup(group_id='bv_stg_links') as bv_stg_lnk_group:    
            for i,lnk in enumerate(config['bv']['links']):
                bv_stage_lnk = BV_Link_Stager(task_id=f"stage_{lnk}", 
                                        conf=config['bv'],
                                        lnk=lnk)   
                if config['bv']['links'][lnk]['dependencies']:
                    dependency_map[bv_stage_lnk]=[x for x in config['bv']['links'][lnk]['dependencies']]
                    
                task_list[f'stage__{lnk}']=bv_stage_lnk
                
        with TaskGroup(group_id='bv_stg_sats') as bv_stg_sat_group:    
            for i,sat in enumerate(config['bv']['sats']):
                bv_stage_sat = BV_Sat_Stager(task_id=f"stage_{sat}", 
                                        conf=config['bv'],
                                        sat=sat)                 
                if config['bv']['sats'][sat]['dependencies']:
                    dependency_map[bv_stage_sat]=[x for x in config['bv']['sats'][sat]['dependencies']]
                    
                task_list[f'stage__{sat}']=bv_stage_sat
                    
    # ***********************
    #       BV-LOADING
    # ***********************
    with TaskGroup(group_id='LOAD_BUSINESS_VAULT') as bv_group:
        # ***********************
        #       BV HUBS
        # ***********************
        with TaskGroup(group_id='bv_load_hubs') as bv_load_hubs:
            for i,hub in enumerate(config['bv']['hubs']):                                
                load_hub = Hub_Loader(task_id=f"load_{hub}", 
                                    conf=config['bv'],
                                    hub=hub)  
                  
                task_list[f'load__{hub}']=load_hub
                dependency_map[load_hub]=[f'stage__{hub}']
                
        # ***********************
        #       BV LINKS
        # ***********************
        with TaskGroup(group_id='bv_load_links') as bv_load_links:
            for i,lnk in enumerate(config['bv']['links']):                                
                load_link = Link_Loader(task_id=f"load_{lnk}", conf=config['bv'],lnk=lnk)
                
                task_list[f'load__{lnk}']=load_link  
                dependency_map[load_link]=[f'stage__{lnk}']
                
        # ***********************
        #       BV SATS
        # ***********************
        with TaskGroup(group_id='bv_load_sats') as bv_load_sats:
            for i,sat in enumerate(config['bv']['sats']):                                
                load_sat = Sat_Loader(task_id=f"load_{sat}", conf=config['bv'],sat=sat)
                task_list[f'load__{sat}']=load_sat     
                dependency_map[load_sat]=[f'stage__{sat}']
                                       
                
    read_config >> set_vars(conf=config) >> connect(conf=config['connection']) >> \
                   get_tables(conns=config['connection']) >> staging_group >> rv_group >> \
                   bv_staging_group # >> bv_group # das letzte geht nicht sonst zykl. dependency

    # add dependencies to bv-tasks
    print("\ndependency map:\n")
    for key,val in dependency_map.items():
        print(key,val)
    print("\n")
    print("\ntask list:\n")
    for i in task_list:
        print(i)
    
    task_keys = task_list.keys()
    if dependency_map:
        for key,val in dependency_map.items():
            # map the vals to handles within task_list        
            matching_items=[]
            for search_string in val:
                # achtung: hier gibts noch kein error-handling, falls search_string mal nicht gefunden wird
                matching_list = list(filter(lambda x: search_string in x,task_keys))
                if matching_list:
                    matching_items.append(matching_list[0])
            
            if matching_items:
                matching_handles = []
                for item in matching_items:
                    matching_handles.append(task_list[item])
                
                print("\nMatching handles:")
                print(matching_handles)
                key.set_upstream(matching_handles)


globals()[DAG_ID] = airvault()