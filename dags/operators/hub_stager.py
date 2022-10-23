from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict

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
        
        for src in conf['rv']['hubs'][hub]['src'].keys():            
            sql=f"""SELECT column_name, data_type 
                   FROM information_schema.columns
                   WHERE table_schema = \'{schema_source}\'
                   AND table_name   = \'{src}\';"""
            
            records=self.hook.get_records(sql)
            src_cols[src]=[i[0] for i in records]
            src_hub[hub].append(src)                        
                    
        #print(src_cols)  # verursacht eine info-msg mit loggin_mixin.py als quelle
        #self.log.info(f"{src_cols}")   # info-msg mit hub_stager.py als quelle
        
        conf=conf['rv']
        for hub,tables in src_hub.items():            
            hk=conf['hubs'][hub]['hk']
            for table_name in tables:        
                records=src_cols[table_name]
                bk_tgt=conf['hubs'][hub]['bk']                
                bks_src=conf['hubs'][hub]['src'][table_name]['bks']    
                
                #######################
                #     CREATE SQL
                #######################
                sql=\
                f"""CREATE TABLE IF NOT EXISTS  {schema_stage}.{table_name}__{hub} (
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
                TRUNCATE {schema_stage}.{table_name}__{hub};
                """
                
                sql_concat=sql
                self.hook.run(sql) 
                
                
                ######################
                #   INSERT SQL
                #######################        
                bk_trans = [f"trim(coalesce({x}::varchar({maxvarchar}),\'-1\'))" for x in bks_src] #-1= zero key
                bk_trans_str=",".join(bk_trans)                        
                
                tenant = conf['hubs'][hub]['src'][table_name]['tenant']
                if not tenant:
                    tenant='default'
                    
                bkeycode = conf['hubs'][hub]['src'][table_name]['bkeycode']
                if not bkeycode:
                    bkeycode='default'    
                    
                    
                # multiple bkeys map to one?             
                bk_str = f"concat_ws('|',{bk_trans_str})"  # only pure bk
                hk_str = f"concat_ws('|','{tenant}','{bkeycode}',{bk_trans_str})"
                hk_str = f'md5({hk_str})'
                
                # folgende loop behandelt Zero-Keys
                records2=[]        
                for x in records:            
                    if x in bks_src:
                        #if not bk_str in records2:
                            #records2.append(bk_str)                
                        records2.append(f'trim(coalesce({x}::varchar({maxvarchar}),\'-1\'))') # -1=zero key
                    else:
                        records2.append(f'trim({x}::varchar({maxvarchar}))')                        

                #original_fields = ',\n\t'.join(records2)
                if not appts:
                    appts='current_timestamp'
                else:
                    appts=appts+'::timestamp'                
                        
                
                sql=f"""INSERT INTO {schema_stage}.{table_name}__{hub} SELECT
                --bk--
                {bk_str},
                --hk--
                {hk_str},
                --meta--        
                current_timestamp,
                {appts},
                \'{table_name}\',
                \'{tenant}\',
                \'{bkeycode}\',
                \'{task_id}\'
                FROM {schema_source}.{table_name}
                ;        
                """
                                                
                self.hook.run(sql) 
                
                sql_concat += '\n'+sql
                self.doc = sql_concat
        
                
        #self.hook.run(self.sql)                
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        