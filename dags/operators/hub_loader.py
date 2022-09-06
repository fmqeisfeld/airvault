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
                 *args,
                 **kwargs):
        
        self.doc_md = __doc__
        super().__init__(*args, **kwargs)
        
        
        tgt_table=list(conf.keys())[0]
        self.tgt_table=tgt_table
        
        self.tgt_bk=conf[tgt_table]['bk']
        self.tgt_type=conf[tgt_table]['type']
        
        self.src_bk=conf[tgt_table]['src']['bk']
        self.src_table=conf[tgt_table]['src']['table']                   
                             
        self.sql= open('/opt/airflow/dags/sql/hub_loader.sql','r').read()
        
        params = {'src_bk':self.src_bk, 
                  'src_table':self.src_table}
        
        self.sql = self.sql.format(**params)

    def execute(self, context: Context): 
        avail_tables =  Variable.get('vault_tables')
        
        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        #self.hook.run(self.sql)
        result=self.hook.get_records(self.sql)
        
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        