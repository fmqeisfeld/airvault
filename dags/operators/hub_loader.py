from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict
from jinja2 import Template


class Hub_Loader(BaseOperator):  
    """ TEST DOC   """    
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 hub:str,                 
                 *args,
                 **kwargs):
        
        self.doc_md = __doc__
        self.sql=""
        super().__init__(*args, **kwargs)
                             
        schema_edwh = Variable.get('SCHEMA_EDWH')                             
        schema_stage = Variable.get('SCHEMA_STAGE')
        schema_source = Variable.get('SCHEMA_SOURCE')
        
        maxvarchar = Variable.get('MAXVARCHAR')        
        appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                        
        task_id=self.task_id
                
        src_cols=defaultdict(list,{})
        src_hub=defaultdict(list,{})           
        self.hook = PostgresHook(postgres_conn_id='pgconn')     
        
        for src in conf['hubs'][hub]['src'].keys():
            src_hub[hub].append(src)  
            
        #conf=conf['rv'] # wird bereits in dag gemacht
        
        for hub,tables in src_hub.items():            
            hk=conf['hubs'][hub]['hk']
            bk_tgt=conf['hubs'][hub]['bk']
            #######################
            #     CREATE SQL
            #######################

            sql_template = open('/opt/airflow/dags/sql/hub_loader_create.sql','r').read()
            context = {
                "schema_edwh":schema_edwh,
                "bk_tgt":bk_tgt,
                "maxvarchar":maxvarchar,    
                "hk":hk,
                "hub":hub
            }
            template = Template(sql_template)
            sql=template.render(**context)
            self.sql += sql
            #print(sql)
            #continue
            
            
            for table_name in tables:                                        
                ######################
                #   INSERT SQL
                #######################                                   
                sql_template = open('/opt/airflow/dags/sql/hub_loader_insert.sql','r').read()
                
                context = {
                    "schema_edwh":schema_edwh,
                    "schema_stage":schema_stage,            
                    "bk_tgt":bk_tgt,
                    "table_name":table_name,            
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "hub":hub,
                    "task_id":task_id
                }
            
                template = Template(sql_template)
                sql=template.render(**context)
                self.sql += f"\n{sql}"
        
        self.doc = self.sql
                    
    def execute(self, context: Context):                                
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)
        
        
        
            
        