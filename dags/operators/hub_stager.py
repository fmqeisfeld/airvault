from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict
from jinja2 import Template

class Hub_Stager(BaseOperator):  
    """ TEST DOC FOR HUB LOADER   """    
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 hub:str,                 
                 *args,
                 **kwargs):
        
        self.doc_md = __doc__
        super().__init__(*args, **kwargs)
                             
        self.schema_source = Variable.get('SCHEMA_SOURCE')                             
        self.schema_stage = Variable.get('SCHEMA_STAGE')
        self.maxvarchar = Variable.get('MAXVARCHAR')
        self.hub=hub
        self.appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                        
        self.conf=conf
                        

    def execute(self, context: Context):        
        
        self.hook = PostgresHook(postgres_conn_id='pgconn')
        sql_concat=""
        
        src_cols=defaultdict(list,{})
        src_hub=defaultdict(list,{})
        conf=self.conf
        hub=self.hub
        schema_source=self.schema_source
        schema_stage=self.schema_stage
        
        maxvarchar=self.maxvarchar
        appts=self.appts
        task_id=self.task_id
        
        for src in conf['hubs'][hub]['src'].keys():            
            sql=f"""SELECT column_name, data_type 
                   FROM information_schema.columns
                   WHERE table_schema = \'{schema_source}\'
                   AND table_name   = \'{src}\';"""
            
            records=self.hook.get_records(sql)
            src_cols[src]=[i[0] for i in records]
            src_hub[hub].append(src)                        
                    
        #print(src_cols)  # verursacht eine info-msg mit loggin_mixin.py als quelle
        #self.log.info(f"{src_cols}")   # info-msg mit hub_stager.py als quelle
        
        #conf=conf['rv'] # wird bereits in dag gemacht
        
        for hub,tables in src_hub.items():            
            hk=conf['hubs'][hub]['hk']
            for table_name in tables:        
                records=src_cols[table_name]
                bk_tgt=conf['hubs'][hub]['bk']                
                bks_src=conf['hubs'][hub]['src'][table_name]['bks']    

                #######################
                #     CREATE SQL
                #######################
                sql_template = open('/opt/airflow/dags/sql/hub_stager_create.sql','r').read()                        
                context = {
                    "schema_stage":schema_stage,
                    "table_name":table_name,
                    "bk_tgt":bk_tgt,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "hub":hub
                }
                template = Template(sql_template)
                sql=template.render(**context)

                sql_concat += f"\n{sql}"
                #continue

                ######################
                #   INSERT SQL
                #######################        
                bk_trans = [f"trim(coalesce({x}::varchar({maxvarchar}),\'-1\'))" for x in bks_src] #-1= zero key                                   

                tenant = conf['hubs'][hub]['src'][table_name]['tenant']
                if not tenant:
                    tenant='default'

                bkeycode = conf['hubs'][hub]['src'][table_name]['bkeycode']
                if not bkeycode:
                    bkeycode='default'    
                        

                if not appts:
                    appts='current_timestamp'                       
                
                sql_template=open('/opt/airflow/dags/sql/hub_stager_insert.sql','r').read()
                
                context = {
                    "schema_stage":schema_stage,
                    "schema_source":schema_source,
                    "table_name":table_name,
                    "bks_src":bks_src,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "hub":hub,
                    "appts":appts,
                    "tenant":tenant,
                    "bkeycode":bkeycode,
                    "task_id":task_id
                }
                template = Template(sql_template)
                sql=template.render(**context)
                
                sql_concat += '\n'+sql       
                                                                
                
        self.doc = sql_concat                        
        self.hook.run(sql_concat)                
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        