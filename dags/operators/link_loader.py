from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from collections import defaultdict

class Link_Loader(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 lnk:str,                           
                 *args,
                 **kwargs):
                
        super().__init__(*args, **kwargs)
                             
        schema_edwh = Variable.get('SCHEMA_EDWH')                             
        schema_stage = Variable.get('SCHEMA_STAGE')
        schema_source = Variable.get('SCHEMA_SOURCE')
        
        maxvarchar = Variable.get('MAXVARCHAR')        
        appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                        
        task_id=self.task_id
                
        src_lnk_cols=defaultdict(list,{})
        src_lnk=defaultdict(list,{})           
        self.hook = PostgresHook(postgres_conn_id='pgconn')  
        
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
                    
            hks_list=[f"{x} varchar({maxvarchar}) NOT NULL" for x in hks]
            hks_def_list_str=",\n\t".join(hks_list)    
            hks_val_list_str=",\n\t".join(hks)    

            cks_def_list_str = '--- NONE ---'
            cks_val_list_str = '--- NONE ---'
                        
            if 'cks' in conf['links'][link]:
                cks=conf['links'][link]['cks'].values()

                if len(cks)>0:
                    cks_list=[f"{x} varchar({maxvarchar})" for x in cks]
                    cks_def_list_str=",\n\t".join(cks_list)
                    cks_val_list_str=",\n\t".join(cks)
            
            
            #######################
            #     CREATE SQL
            #######################
            sql=f"""CREATE TABLE IF NOT EXISTS  {schema_edwh}.{link} (
            --hk--
            {hk} varchar({maxvarchar}) NOT NULL,
            --hks--
            {hks_def_list_str},
            --cks--
            {cks_def_list_str},    
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
                sql=f"""INSERT INTO {schema_edwh}.{link} SELECT
                --hk--
                {hk},
                --hks--
                {hks_val_list_str},        
                --cks--
                {cks_val_list_str},
                --meta--
                current_timestamp,
                DV_APPTS,
                DV_RECSRC,
                DV_TENANT,
                DV_BKEYCODE,        
                '{task_id}'
                FROM {schema_stage}.{table_name}__{link} src
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {schema_edwh}.{link} tgt
                    WHERE src.{hk} = tgt.{hk}
                )
                ;        
                """
                self.sql += f"\n{sql}"
        
        
        self.doc=self.sql


    def execute(self, context: Context):        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)

        
            
        