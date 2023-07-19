from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from collections import defaultdict
from jinja2 import Template

class Link_Loader(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 lnk:str,                           
                 *args,
                 **kwargs):
                
        super().__init__(*args, **kwargs)
                             
        self.schema_edwh = Variable.get('SCHEMA_EDWH')                             
        self.schema_stage = Variable.get('SCHEMA_STAGE')
        self.schema_source = Variable.get('SCHEMA_SOURCE')
        
        self.maxvarchar = Variable.get('MAXVARCHAR')        
        self.appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                                
        self.lnk = lnk                        
        self.conf = conf



    def execute(self, context: Context):     
        src_lnk_cols=defaultdict(list,{})
        src_lnk=defaultdict(list,{})           
                
        conf = self.conf
        lnk = self.lnk 
        schema_source = self.schema_source
        maxvarchar = self.maxvarchar
        schema_edwh = self.schema_edwh
        schema_stage = self.schema_stage
        self.hook = PostgresHook(postgres_conn_id='pgconn')
        
        sql_concat=""
        
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
            hk=conf['links'][link]['hk']
            hks=conf['links'][link]['hks']

            cks=[]
            if 'cks' in conf['links'][link]:
                cks=conf['links'][link]['cks'].values()


            #######################
            #     CREATE SQL
            #######################    
            sql_template=open('/opt/airflow/dags/sql/link_loader_create.sql','r').read()
            context = {
                "schema_edwh":schema_edwh,
                "link":link,
                "maxvarchar":maxvarchar,    
                "hk":hk,
                "hks":hks,
                "cks":cks        
            }
            template = Template(sql_template)
            sql=template.render(**context)
            sql_concat += f"\n{sql}"
            #print(sql)
            #continue
            
            
            for table_name in tables:                                                                                        
                ######################
                #   INSERT SQL
                #######################                                                   
                sql_template=open('/opt/airflow/dags/sql/link_loader_insert.sql','r').read()
                context = {
                "schema_edwh":schema_edwh,
                "schema_stage":schema_stage,
                "link":link,
                "maxvarchar":maxvarchar,    
                "hk":hk,
                "hks":hks,
                "cks":cks,
                "table_name":table_name,
                "task_id":self.task_id
                }
                
                template = Template(sql_template)
                sql=template.render(**context)

                sql_concat += f"\n{sql}"
        
        
        self.doc=sql_concat
        
        self.hook.run(sql_concat)

        
            
        