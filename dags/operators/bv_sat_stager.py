from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict
from jinja2 import Template

class BV_Sat_Stager(BaseOperator):  
    """ TEST DOC   """    
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 sat:str,                 
                 *args,
                 **kwargs):
        
        self.doc_md = __doc__
        super().__init__(*args, **kwargs)
                             
        self.schema_source = Variable.get('SCHEMA_SOURCE')                             
        self.schema_stage = Variable.get('SCHEMA_STAGE')
        self.maxvarchar = Variable.get('MAXVARCHAR')
        self.sat=sat
        self.appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                        
        self.conf=conf
                        

    def execute(self, context: Context):        
        
        self.hook = PostgresHook(postgres_conn_id='pgconn')
        sql_concat=""
        
        src_sat_cols=defaultdict(list,{})
        src_sat=defaultdict(list,{})
        
        conf=self.conf
        sat=self.sat
        schema_source=self.schema_source
        schema_stage=self.schema_stage
        
        maxvarchar=self.maxvarchar
        appts=self.appts
        task_id=self.task_id
        
        # conf=conf['bv'] # wird bereits in dag gemacht
              
        for src in conf['sats'][sat]['src'].keys():
            src_sat[sat].append(src)                                   
                
        
        for sat,tables in src_sat.items():            
            hk=conf['sats'][sat]['hk']
            if conf['sats'][sat]['cks'] and conf['sats'][sat]['multiactive']:
                self.log.error(f"multiactive satellite {sat} must not have dependent child keys '")
                                
            for table_name in tables:
                # attrs
                attrs = conf['sats'][sat]['attrs']


                # ckeys
                cks=[]
                if conf['sats'][sat]['cks']:
                    cks = (conf['sats'][sat]['cks']).values()
                    #print(cks)

                #######################
                #     CREATE SQL
                #######################
                sql_template=open('/opt/airflow/dags/sql/sat_stager_create.sql','r').read()

                context = {
                    "schema_stage":schema_stage,
                    "table_name":table_name,            
                    "sat":sat,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "cks":cks,
                    "attrs":attrs
                }
                
                
                template = Template(sql_template)
                sql=template.render(**context)
                
                sql_concat += f"\n{sql}"
                                
                
                ######################
                #   INSERT SQL
                #######################   
                tenant = conf['sats'][sat]['src'][table_name]['tenant']
                if not tenant:
                    tenant='default'

                bkeycode = conf['sats'][sat]['src'][table_name]['bkeycode']
                if not bkeycode:
                    bkeycode='default'    

                # hk
                bks=conf['sats'][sat]['src'][table_name]['bks']


                # attr
                attr_mapping = conf['sats'][sat]['src'][table_name]['attrs']                
                attr_mapping_inv={y:x for x,y in attr_mapping.items()}        
                attrs = attr_mapping_inv.values()
                


                #cks   
                ######################################################################
                # ACHTUNG: sql-statement MUSS alle definierten ckey-felder auch wirklich enthalten
                #          also anders als bei rv-links, wo manche quellen evtl. den ck gar nicht haben
                #          -> zero-key treatment
                ####################################################################                  
                cks=[]
                if conf['sats'][sat]['cks']:
                    cks=(conf['sats'][sat]['cks']).keys()


                if not appts:
                    appts='current_timestamp'              

                custom_sql=conf['sats'][sat]['src'][table_name]['sql']

                sql_template = open('/opt/airflow/dags/sql/bv_sat_stager_insert.sql','r').read()
                
                context = {
                    "schema_stage":schema_stage,
                    "table_name":table_name,            
                    "sat":sat,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "cks":cks,
                    "bks":bks,
                    "attrs":attrs,
                    "appts":appts,
                    "tenant":tenant,
                    "bkeycode":bkeycode,
                    "task_id":task_id,
                    "custom_sql":custom_sql
                }
                
                
                template = Template(sql_template)
                sql=template.render(**context)
            
                sql_concat += f"\n{sql}"
                
        self.hook.run(sql_concat)
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        