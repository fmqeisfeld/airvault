from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class Link_Loader(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 link:str,                           
                 *args,
                 **kwargs):
                
        super().__init__(*args, **kwargs)
                             
        link_bks_dict=conf['links'][link]['src']['bks']
        bk_concat_str="CONCAT_WS('|'"
        bk_list=''
        
        for key,val in link_bks_dict.items():    
            bk_list += key+','                
            bk_concat_str+=f",trim(src.{key}::varchar(256))"

        bk_concat_str+=')'
        bk_list=bk_list[:-1]  # rm trailing comma    

        params = {
            'bk_concat_str':bk_concat_str,
            'bk_list':bk_list,            
            'schema_src':conf['connection']['schemas']['stage'],
            'table_src':conf['links'][link]['src']['table'],
            'schema_tgt':conf['connection']['schemas']['edwh'],
            'table_tgt':link,
            'hk_tgt': conf['links'][link]['hk']
        }


        self.sql= open('/opt/airflow/dags/sql/link_loader.sql','r').read()
                        
        self.sql = self.sql.format(**params)
        self.doc=self.sql


    def execute(self, context: Context):        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)

        
            
        