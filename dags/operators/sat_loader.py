from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from collections import defaultdict
from jinja2 import Template


class Sat_Loader(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 sat:str,                           
                 *args,
                 **kwargs):
                
        super().__init__(*args, **kwargs)
                             
        self.schema_edwh = Variable.get('SCHEMA_EDWH')                             
        self.schema_stage = Variable.get('SCHEMA_STAGE')
        self.schema_source = Variable.get('SCHEMA_SOURCE')
        
        self.maxvarchar = Variable.get('MAXVARCHAR')        
        self.appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                        
        self.task_id=self.task_id
        self.sat = sat
                        
        self.conf = conf
        # conf=conf['rv'] # wird bereits in dag gemacht        

    def execute(self, context: Context):        
        
        sql_concat = ""
        schema_edwh = self.schema_edwh
        schema_stage = self.schema_stage
        schema_source = self.schema_source
        maxvarchar = self.maxvarchar
        appts = self.appts
        task_id = self.task_id
        sat = self.sat
        
        src_sat_cols=defaultdict(list,{})
        src_sat=defaultdict(list,{})           
        conf = self.conf

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

                # multiactive
                multiactive=False
                if conf['sats'][sat]['multiactive']:
                    multiactive=True                                        

                #######################
                #     CREATE SQL
                #######################
                sql_template=sql_template=open('/opt/airflow/dags/sql/sat_loader_create.sql','r').read()
                
                context = {
                    "schema_stage":schema_stage,
                    "schema_edwh":schema_edwh,
                    "table_name":table_name,            
                    "sat":sat,
                    "maxvarchar":maxvarchar,    
                    "hk":hk,
                    "cks":cks,
                    "attrs":attrs,
                    "multiactive":multiactive
                }
                
                
                template = Template(sql_template)
                sql=template.render(**context)
            
                sql_concat += f"\n{sql}"
                #continue
                
                ######################
                #   INSERT SQL
                #######################   
                
                ##-------------------
                ## DEPENDENT KEY SAT
                ##-------------------        
                for table_name in tables:
                    if conf['sats'][sat]['cks']:   
                        cks=conf['sats'][sat]['cks'].values()

                        sql_template= open('/opt/airflow/dags/sql/sat_loader_ckey_insert.sql','r').read()

                        context = {
                            "schema_edwh":schema_edwh,
                            "schema_stage":schema_stage,
                            "table_name":table_name,
                            "sat":sat,
                            "maxvarchar":maxvarchar,    
                            "hk":hk,
                            "cks":cks,
                            "attrs":attrs,
                            "task_id":task_id
                        }
                        template = Template(sql_template)
                        sql=template.render(**context)
                        
                        sql_concat += f"\n{sql}"
                        #continue
                    
                    ##------------------
                    ## MULTI-ACTIVE SAT
                    ##---------------------        
                    elif conf['sats'][sat]['multiactive']:
                        # cte's needed
                        sql_template =sql_template=open('/opt/airflow/dags/sql/sat_loader_multiactive_insert.sql','r').read()
                        
                        context = {
                            "schema_edwh":schema_edwh,
                            "schema_stage":schema_stage,
                            "table_name":table_name,
                            "sat":sat,
                            "maxvarchar":maxvarchar,    
                            "hk":hk,
                            "cks":cks,
                            "attrs":attrs,
                            "task_id":task_id
                        }
                        template = Template(sql_template)
                        sql=template.render(**context)
                        sql_concat += f"\n{sql}"
                    ##------------------
                    ## REGULAR SAT
                    ##------------------
                    else:
                        sql_template=open('/opt/airflow/dags/sql/sat_loader_regular_insert.sql','r').read()
                        context = {
                            "schema_edwh":schema_edwh,
                            "schema_stage":schema_stage,
                            "table_name":table_name,
                            "sat":sat,
                            "maxvarchar":maxvarchar,    
                            "hk":hk,
                            "cks":cks,
                            "attrs":attrs,
                            "task_id":task_id
                        }
                        template = Template(sql_template)
                        sql=template.render(**context)
                        sql_concat += f"\n{sql}"
        
        self.doc=sql_concat                        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(sql_concat)

        
            
        