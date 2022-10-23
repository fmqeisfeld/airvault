from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from collections import defaultdict

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
        
        for src in conf['rv']['links'][lnk]['src'].keys(): 
            
            sql=f"""SELECT column_name, data_type 
                   FROM information_schema.columns
                   WHERE table_schema = \'{schema_source}\'
                   AND table_name   = \'{src}\';"""

            records=self.hook.get_records(sql)
            
            src_lnk_cols[src]=[i[0] for i in records]
            src_lnk[lnk].append(src)                    
        
        conf=conf['rv']
        
        for link,tables in src_lnk.items():                
            
            hk=conf['links'][link]['hk']    # PK of link
            hks = conf['links'][link]['hks'] # PKs of hubs
            hks_list = [f"{x} varchar({maxvarchar}) NOT NULL" for x in hks]
            hks_list_str = ",\n\t".join(hks_list)
            
            for table_name in tables:          
                
                bks= conf['links'][link]['src'][table_name]['bks']
                bks_list = [f"{x} varchar({maxvarchar}) NOT NULL" for x in bks]
                        
                
                cks=conf['links'][link]['cks']        
                cks_list=[x for x in cks.values()]
                cks_str=''
                if len(cks_list)>0:
                    cks_str=f" varchar({maxvarchar}) NOT NULL,\n\t".join(cks_list) + f" varchar({maxvarchar}) NOT NULL,"    
                
                #######################
                #     CREATE SQL
                #######################
                sql=f"""CREATE TABLE IF NOT EXISTS {schema_stage}.{table_name}__{lnk} (
                --hk--
                {hk} varchar({maxvarchar}) NOT NULL,
                --hks--
                {hks_list_str},
                --cks--
                {cks_str}        
                --meta--
                DV_LOADTS timestamp NULL,
                DV_APPTS timestamp NULL,
                DV_RECSRC varchar({maxvarchar}) NULL,
                DV_TENANT varchar({maxvarchar}) NULL,
                DV_BKEYCODE varchar({maxvarchar}) NULL,
                DV_TASKID varchar({maxvarchar}) NULL
                );
                TRUNCATE {schema_stage}.{table_name}__{lnk};
                """
                
                sql_concat=sql
                self.hook.run(sql) 
                
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
                    
                hk_val_list=[] # total concatted value for link pk
                hks_val_list=[] # individual concatted vals for hubs pks
                for key in hks:                    
                    # zero-key treatment for non-existent bk
                    if not bks_mapping_inv[key][0] in src_lnk_cols[table_name]:
                        bks_mapping_inv[key]=["\'-1\'"]                    
                        
                    bks_list = [f"coalesce(trim({x}::varchar({maxvarchar})),\'-1\')" for x in bks_mapping_inv[key]]
                    bks_list_joined = ",".join(bks_list)
                    bks_list_str=f"concat_ws('|','{tenant}','{bkeycode}',{bks_list_joined})"
                    bks_list_str=f"md5({bks_list_str})"
                    hks_val_list.append(bks_list_str)
                    
                    hk_val_list.append(bks_list_joined)

                # hks
                hks_val_list_str = ",\n\t".join(hks_val_list)
                # hk        
                hk_val_list_str=",".join(hk_val_list)
                hk_val_list_str=f"md5(concat_ws('|','{tenant}','{bkeycode}',{hk_val_list_str}))"        
                                
                #cks 
                records=src_lnk_cols[table_name]
                cks_val=[f"coalesce(trim({x}::varchar({maxvarchar})),'-1')" if x in records else "'-1'" for x in cks.keys()]
                cks_val_str=",\n\t".join(cks_val)

                if not appts:
                    appts='current_timestamp'
                else:
                    appts=appts+'::timestamp'                
                                
                
                sql=f"""INSERT INTO {schema_stage}.{table_name}__{link} SELECT 
                --hk--
                {hk_val_list_str},
                --hks--
                {hks_val_list_str},
                --cks--
                {cks_val_str},
                --meta--
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
        
        
            
        