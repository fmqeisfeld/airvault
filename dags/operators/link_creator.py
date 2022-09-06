from airflow.models import BaseOperator
from airflow.models.taskinstance import Context

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import Variable

class Link_Creator(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,
                 link:str,                                   
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.link=link
        self.sql= open('/opt/airflow/dags/sql/link_creator.sql','r').read()
        
        # dynamically create list of bk's
        link_bks_dict=conf['links'][link]['src']['bks']
        link_bks=''        
        for key,val in link_bks_dict.items():        
            link_bks += key + ' ' + val['type'] +' NOT NULL ,'    
        
        params = {
            'schema':conf['connection']['schemas']['edwh'],
            'link':link,
            'hk':conf['links'][link]['hk'],
            'bk_list':link_bks
        }        

        self.sql = self.sql.format(**params)
        self.doc = self.sql

    def execute(self, context: Context): 
        vault_tables = Variable.get('vault_tables')

        if not self.link in vault_tables:
            self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
            self.hook.run(self.sql)            