from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict

class Sat_Stager(BaseOperator):  
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
        
        conf=conf['rv']
           
        for sat,sat_obj in conf['sats'].items():
            for src in sat_obj['src'].keys():
                
                sql=f"""SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_schema = \'{schema_source}\'
                    AND table_name   = \'{src}\';"""

                records=self.hook.get_records(sql)
                
                src_sat_cols[src]=[i[0] for i in records]
                src_sat[sat].append(src)                  
        
        
        
        for sat,tables in src_sat.items():            
            hk=conf['sats'][sat]['hk']
            if conf['sats'][sat]['cks'] and conf['sats'][sat]['multiactive']:
                self.log.error(f"multiactive satellite {sat} must not have dependent child keys '")
                break
            for table_name in tables:
                # attrs
                attrs = conf['sats'][sat]['attrs']
                attrs_list = [f"{x} varchar({maxvarchar})" for x in attrs]
                attrs_list_str = ",\n\t".join(attrs_list)                                               
                
                # ckeys
                if conf['sats'][sat]['cks']:
                    cks_mapping=conf['sats'][sat]['cks']
                    cks_list=[f"{x} varchar({maxvarchar})" for x in cks_mapping.values()]
                    cks_list_str = ",\n\t".join(cks_list)
                else:
                    cks_list_str="--NONE--"
                    
                
                #######################
                #     CREATE SQL
                #######################
                sql=f"""CREATE TABLE IF NOT EXISTS {schema_stage}.{table_name}__{sat} (
                --hk--
                {hk} varchar({maxvarchar}) NOT NULL,   
                --cks--
                {cks_list_str},
                --attributes--
                {attrs_list_str},
                --meta--
                DV_HASHDIFF varchar({maxvarchar}) NULL,
                DV_LOADTS timestamp NULL,
                DV_APPTS timestamp NULL,
                DV_RECSRC varchar({maxvarchar}) NULL,
                DV_TENANT varchar({maxvarchar}) NULL,
                DV_BKEYCODE varchar({maxvarchar}) NULL,
                DV_TASKID varchar({maxvarchar}) NULL        
                );
                TRUNCATE {schema_stage}.{table_name}__{sat};
                """
                
                sql_concat=sql
                self.hook.run(sql) 
                
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
                hk_val_list = [f"coalesce(trim({x}::varchar({maxvarchar})),'-1')" for x in bks]
                hk_val_list_str=",".join(hk_val_list)
                hk_val_list_str=f"concat_ws('|','{tenant}','{bkeycode}',{hk_val_list_str})"
                hk_val_list_str=f"md5({hk_val_list_str})"
                
                
                # attr
                attr_mapping = conf['sats'][sat]['src'][table_name]['attrs']                
                attr_mapping_inv={y:x for x,y in attr_mapping.items()}
                attr_val_list = [f"{x}" for x in attr_mapping_inv.values()]
                attr_val_list_str=",\n\t".join(attr_val_list)
                
                # hashdiff
                hashdiff_list = [f"trim({x}::varchar({maxvarchar}))" for x in attr_mapping_inv.values()]
                hashdiff_list_str = "md5(concat_ws('|'," + ",".join(hashdiff_list) +"))"
                                                
                
                #cks   
                if conf['sats'][sat]['cks']:
                    cks_val_list=[f"coalesce(trim({x}::varchar({maxvarchar})),\'-1\')" if x in src_sat_cols[table_name] else "\'-1\'" for x in cks_mapping.keys()]        
                    cks_val_list_str=",\n\t".join(cks_val_list)
                else:
                    cks_val_list_str="--NONE--"
                                                    
                if not appts:
                    appts='current_timestamp'
                else:
                    appts=appts+'::timestamp'                
                        
                
                sql=f"""INSERT INTO {schema_stage}.{table_name}__{sat} SELECT
                --hk--
                {hk_val_list_str},  
                --cks--
                {cks_val_list_str},           
                --attributes--
                {attr_val_list_str},  
                --meta--        
                {hashdiff_list_str},
                current_timestamp,
                {appts},
                '{table_name}',
                '{tenant}',
                '{bkeycode}',
                '{task_id}'
                FROM {schema_source}.{table_name}
                ;        
                """
                
                self.hook.run(sql)                 
                sql_concat += '\n'+sql
                self.doc = sql_concat        
                
        #self.hook.run(self.sql)                
        #context['ti'].xcom_push(key='records', value=result)        
        #Variable.set('myvar',{'mykey':'myval'})
        
        
            
        