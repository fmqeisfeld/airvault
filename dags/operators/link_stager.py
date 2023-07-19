from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict
from jinja2 import Template

class Link_Stager(BaseOperator):  
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
            
            sql=f"""SELECT column_name, data_type 
                   FROM information_schema.columns
                   WHERE table_schema = \'{schema_source}\'
                   AND table_name   = \'{src}\';"""

            records=self.hook.get_records(sql)
            
            src_lnk_cols[src]=[i[0] for i in records]
            src_lnk[lnk].append(src)                    
        
        # conf=conf['rv'] # wird bereits in dag gemacht
                
        for link,tables in src_lnk.items():                

            hk=conf['links'][link]['hk']    # PK of link
            hks = conf['links'][link]['hks'] # PKs of hubs

            for table_name in tables:          
                                        
                cks=[]
                if 'cks' in conf['links'][link]:
                    cks=conf['links'][link]['cks'].values()


                #######################
                #     CREATE SQL
                #######################
                sql_template=open('/opt/airflow/dags/sql/link_stager_create.sql','r').read()
                context = {
                    "schema_stage":schema_stage,
                    "link":link,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "hks":hks,
                    "cks":cks,
                    "table_name":table_name,
                    "lnk":link 
                }
                template = Template(sql_template)
                sql=template.render(**context)        
                sql_concat += f"\n{sql}"
                #print(sql)
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

                
                for key in hks:                    
                    # zero-key treatment for non-existent bk
                    if not bks_mapping_inv[key][0] in src_lnk_cols[table_name]:
                        bks_mapping_inv[key]=["\'-1\'"]                    

                #cks 
                cks=[]
                if 'cks' in conf['links'][link]:                            
                    records=src_lnk_cols[table_name]
                    cks = conf['links'][link]['cks'].keys()

                if not appts:
                    appts='current_timestamp'

            
                sql_template=open('/opt/airflow/dags/sql/link_stager_insert.sql','r').read()
                context = {
                    "schema_stage":schema_stage,
                    "schema_source":schema_source,
                    "link":link,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "hks":hks,
                    "cks":cks,
                    "table_name":table_name,
                    "link":link,
                    "appts":appts,
                    "tenant":tenant,
                    "bkeycode":bkeycode,
                    "task_id":task_id,
                    "cks":cks,
                    "records":records,
                    "bks_mapping_inv":bks_mapping_inv
                }
                template = Template(sql_template)
                sql=template.render(**context)                        
                                                                                
                sql_concat += f"\n{sql}"
                
        self.doc = sql_concat
        self.hook.run(sql_concat)                     
        
        
            
        