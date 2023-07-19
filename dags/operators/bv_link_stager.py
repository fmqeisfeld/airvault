from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict
from jinja2 import Template

class BV_Link_Stager(BaseOperator):  
    """ TEST DOC   """    
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 lnk:str,                 
                 *args,
                 **kwargs):
        
        self.doc_md = __doc__
        super().__init__(*args, **kwargs)
                             
        self.schema_source = Variable.get('SCHEMA_SOURCE')                             
        self.schema_stage = Variable.get('SCHEMA_STAGE')
        self.maxvarchar = Variable.get('MAXVARCHAR')
        self.lnk=lnk
        self.appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                        
        self.conf=conf
                        

    def execute(self, context: Context):        
        
        self.hook = PostgresHook(postgres_conn_id='pgconn')
        sql_concat=""
        
        src_lnk_cols=defaultdict(list,{})
        src_lnk=defaultdict(list,{})
        
        conf=self.conf
        lnk=self.lnk
        schema_source=self.schema_source
        schema_stage=self.schema_stage
        
        maxvarchar=self.maxvarchar
        appts=self.appts
        task_id=self.task_id
        
        for src in conf['links'][lnk]['src'].keys():             
            src_lnk[lnk].append(src)                    
                
        for link,tables in src_lnk.items():                

            hk=conf['links'][link]['hk']    # PK of link
            hks = conf['links'][link]['hks'] # PKs of hubs

            for table_name in tables:          
                bks= conf['links'][link]['src'][table_name]['bks']

                cks = []
                if 'cks' in conf['links'][link]:
                    cks=conf['links'][link]['cks'].values()                    


                #######################
                #     CREATE SQL (same as rv link stager create)
                #######################
                sql_template=sql_template=open('/opt/airflow/dags/sql/link_stager_create.sql','r').read()
                
                context = {
                    "schema_stage":schema_stage,            
                    "lnk":lnk,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "hks":hks,
                    "cks":cks,
                    "table_name":table_name,
                    "link":link # is param given to link_loader in prod-code
                }
                template = Template(sql_template)
                sql=template.render(**context)        
                
                sql_concat += f"\n{sql}"
                #continue

                ######################
                #   INSERT SQL
                #######################
                tenant = conf['links'][link]['src'][table_name]['tenant']
                if not tenant:
                    tenant='default'

                bkeycode = conf['links'][link]['src'][table_name]['bkeycode']
                if not bkeycode:
                    bkeycode='default'    


                # hks & hk
                bks_mapping = conf['links'][link]['src'][table_name]['bks']
                bks_mapping_inv=defaultdict(list)
                for i,j in bks_mapping.items():
                    bks_mapping_inv[j].append(i)

                
                bks_list=[]
                for key in hks:   
                    ###############################################                 
                    # ACHTUNG: zero-key treatment geht hier nicht
                    #          Man muss dafür sorgen, dass das custom-sql auch wirklich alle nötigen felder liefert!
                    ###############################################
                    bks_list+=[x for x in bks_mapping_inv[key]]
                

                custom_sql=conf['links'][link]['src'][table_name]['sql']        
                
                if not appts:
                    appts='current_timestamp'                
                                        

                ######################################################################
                # ACHTUNG: sql-statement MUSS alle definierten ckey-felder auch wirklich enthalten
                #          also anders als bei rv-links, wo manche quellen evtl. den ck gar nicht haben
                #          -> zero-key treatment
                ####################################################################                                  
                #cks 
                cks = []
                if 'cks' in conf['links'][link]:
                    cks=conf['links'][link]['cks'].keys() 
                                


                sql_template=open('/opt/airflow/dags/sql/bv_link_stager_insert.sql','r').read()
                
                context = {
                    "schema_stage":schema_stage,            
                    "lnk":lnk,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "hks":hks,
                    "cks":cks,
                    "table_name":table_name,
                    "link":link,
                    "custom_sql":custom_sql,
                    "bks_list":bks_list,
                    "tenant":tenant,
                    "bkeycode":bkeycode,
                    "task_id":task_id,
                    "appts":appts,
                    "custom_sql":custom_sql
                    
                }
                template = Template(sql_template)        
                sql=template.render(**context)                
                sql_concat += f"\n{sql}"
        
                
        self.hook.run(sql_concat)  
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        