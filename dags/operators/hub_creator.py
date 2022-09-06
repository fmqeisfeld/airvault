from airflow.models import BaseOperator
from airflow.models.taskinstance import Context

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import Variable

class Hub_Creator(BaseOperator):  
        
    #@apply_defaults
    def __init__(self,
                 conf:dict,                                   
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)
                

    def execute(self, context: Context): 
        pass
        
        
            
        