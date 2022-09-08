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
                             
        schema_edwh = Variable.get('SCHEMA_EDWH')                             
        schema_stage = Variable.get('SCHEMA_STAGE')
        appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface

        self.sql= open('/opt/airflow/dags/sql/hub_loader.sql','r').read()
        sql_concat=''

        for cur_src,cur_conf in conf['hubs'][hub]['src'].items():

            params={'tenant':cur_conf['tenant'] if cur_conf['tenant'] else "default",
                    'bkeycode':cur_conf['bkeycode'] if cur_conf['bkeycode'] else "default",
                    'taskid': self.task_id,
                    'appts': appts+'::timestamp' if appts else 'current_timestamp',
                    'rec_src':cur_src,
                    'bk_src':cur_conf['bk'],
                    'schema_src':schema_stage,
                    'table_src':cur_src,
                    'bk_tgt':conf['hubs'][hub]['bk'],
                    'hk_tgt':conf['hubs'][hub]['hk'],
                    'schema_tgt':schema_edwh,
                    'table_tgt':hub
            }
                        
            sql_concat += '\n'+self.sql.format(**params)

        self.sql=sql_concat
        self.doc = self.sql

    def execute(self, context: Context):        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)
        #result=self.hook.get_records(self.sql)
        
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        