from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

class Hub_Loader(BaseOperator):  
    """ TEST DOC FOR HUB LOADER   """    
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 hub:str,                           
                 *args,
                 **kwargs):
        
        self.doc_md = __doc__
        super().__init__(*args, **kwargs)
                             
                             
        self.sql= open('/opt/airflow/dags/sql/hub_loader.sql','r').read()
        
        params = {'bk_src':conf['hubs'][hub]['src']['bk'],
                  'rec_src':conf['hubs'][hub]['src']['table'],
                  'schema_src':conf['connection']['schemas']['stage'],
                  'table_src':conf['hubs'][hub]['src']['table'],
                  'schema_tgt':conf['connection']['schemas']['edwh'],
                  'table_tgt':hub,
                  'hk_tgt':conf['hubs'][hub]['hk'],
                  'bk_tgt':conf['hubs'][hub]['src']['bk']}
                
        
        self.sql = self.sql.format(**params)
        self.doc=self.sql


    def execute(self, context: Context):        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)
        #result=self.hook.get_records(self.sql)
        
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        