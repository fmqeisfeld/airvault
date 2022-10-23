from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict


class Hub_Loader(BaseOperator):  
    """ TEST DOC   """    
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 hub:str,                 
                 *args,
                 **kwargs):
        
        self.doc_md = __doc__
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
        
        for src in conf['rv']['hubs'][hub]['src'].keys():
            sql=f"""SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_schema = \'{schema_source}\'
                    AND table_name   = \'{src}\';"""
            
            records=self.hook.get_records(sql)
            src_cols[src]=[i[0] for i in records]
            src_hub[hub].append(src)  
            
        conf=conf['rv']
        
        for hub,tables in src_hub.items():            
            hk=conf['hubs'][hub]['hk']
            bk_tgt=conf['hubs'][hub]['bk']
            #######################
            #     CREATE SQL
            #######################
            sql=f"""CREATE TABLE IF NOT EXISTS {schema_edwh}.{hub} (
            --bk--
            {bk_tgt} varchar({maxvarchar}) NOT NULL,
            --hk--
            {hk} varchar({maxvarchar}) NOT NULL,
            --meta--    
            DV_LOADTS timestamp NULL,
            DV_APPTS timestamp NULL,
            DV_RECSRC varchar({maxvarchar}) NULL,
            DV_TENANT varchar({maxvarchar}) NULL,
            DV_BKEYCODE varchar({maxvarchar}) NULL,
            DV_TASKID varchar({maxvarchar}) NULL        
            );
            """
            
            self.sql=sql
                
            
            for table_name in tables:                                        
                ######################
                #   INSERT SQL
                #######################                                   
                sql=f"""INSERT INTO {schema_edwh}.{hub} SELECT        
                --bk--
                {bk_tgt},        
                --hk--
                {hk},
                --meta--
                current_timestamp,
                DV_APPTS,
                DV_RECSRC,
                DV_TENANT,
                DV_BKEYCODE,        
                '{task_id}'
                FROM {schema_stage}.{table_name}__{hub} src
                WHERE NOT EXISTS (
                SELECT 1 
                FROM {schema_edwh}.{hub} tgt
                WHERE src.{hk} = tgt.{hk}
                )
                ;        
                """
                self.sql += f"\n{sql}"
        
        self.doc = self.sql
                    
    def execute(self, context: Context):        
        
        
        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)
        
        #result=self.hook.get_records(self.sql)
        
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        