from airflow.models import BaseOperator
from airflow.models.taskinstance import Context

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import Variable

class Hub_Creator(BaseOperator):  
    def __init__(self,
                 conf:dict,
                 hub:str,
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.hub=hub
        self.sql= open('/opt/airflow/dags/sql/hub_creator.sql','r').read()
        schema_edwh = Variable.get('SCHEMA_EDWH')

        params = {'schema':schema_edwh,
                  'hub':hub,
                  'hk':conf['hubs'][hub]['hk'],
                  'bk':conf['hubs'][hub]['bk'],
                  }
        
        self.sql = self.sql.format(**params)
        self.doc = self.sql

    def execute(self, context: Context): 
        vault_tables = Variable.get('vault_tables')
        
        if not self.hub in vault_tables:
            self.log.info('trying to create new table')
            self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
            self.hook.run(self.sql)            
                