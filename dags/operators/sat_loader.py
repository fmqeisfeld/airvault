from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from collections import defaultdict

class Sat_Loader(BaseOperator):  
    #@apply_defaults
    def __init__(self,
                 conf:dict,    
                 sat:str,                           
                 *args,
                 **kwargs):
                
        super().__init__(*args, **kwargs)
                             
        schema_edwh = Variable.get('SCHEMA_EDWH')                             
        schema_stage = Variable.get('SCHEMA_STAGE')
        schema_source = Variable.get('SCHEMA_SOURCE')
        
        maxvarchar = Variable.get('MAXVARCHAR')        
        appts = None #Variable.get('APPTS',default_var=None)  # applied dts. Global Var, set via Webinterface                        
        task_id=self.task_id
                
        src_sat_cols=defaultdict(list,{})
        src_sat=defaultdict(list,{})           
        self.hook = PostgresHook(postgres_conn_id='pgconn')  
        
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
                
            # attrs
            attrs = conf['sats'][sat]['attrs']
            attrs_list = [f"{x} varchar({maxvarchar})" for x in attrs]
            attrs_def_list_str = ",\n\t".join(attrs_list)
            attrs_val_list_str = ",\n\t".join(attrs)
            
            # cks
            if conf['sats'][sat]['cks']:
                cks=conf['sats'][sat]['cks'].values()
                cks_def_list=[f"{x} varchar({maxvarchar})" for x in cks]
                cks_def_list_str=",\n\t".join(cks_def_list)
                cks_val_list_str=",\n\t".join(cks)
            else:
                cks_def_list_str='--NONE--'
                cks_val_list_str='--NONE--'
            
            # multiactive
            if conf['sats'][sat]['multiactive']:
                multiactive_def_str=f"DV_SUBSEQ int NOT NULL"
            else:
                multiactive_def_str="--"
                    
            
            #######################
            #     CREATE SQL
            #######################
            sql=f"""CREATE TABLE IF NOT EXISTS {schema_edwh}.{sat} (
            --attributes--
            {attrs_def_list_str},
            --cks--
            {cks_def_list_str},
            --hk--
            {hk} varchar({maxvarchar}) NOT NULL,
            --meta--    
            DV_HASHDIFF varchar({maxvarchar}) NULL,
            DV_LOADTS timestamp NULL,
            DV_APPTS timestamp NULL,
            DV_RECSRC varchar({maxvarchar}) NULL,
            DV_TENANT varchar({maxvarchar}) NULL,
            DV_BKEYCODE varchar({maxvarchar}) NULL,
            {multiactive_def_str},
            DV_TASKID varchar({maxvarchar}) NULL        
            );"""
            
            self.sql=sql
            #sql=sqlparse.format(sql, reindent=True, keyword_case='upper')
                
            
            ######################
            #   INSERT SQL
            #######################                                   
            # sat-type-dependet SQL
            # depends on type of sat: standard, ck or multiactive
            
            ##-------------------
            ## DEPENDENT KEY SAT
            ##-------------------
            for table_name in tables:
                if conf['sats'][sat]['cks']:        
                    cks_list_str=",".join(cks)
                    sql_where_conds_ck_list = [f"AND src.{x}=cur.{x}" for x in cks]
                    sql_where_conds_ck_list_str ="\n\t".join(sql_where_conds_ck_list)

                    sql=\
                    f"""WITH CUR AS (
                        SELECT {hk}
                            , {cks_list_str}
                            , DV_HASHDIFF
                            , RANK() over (PARTITION BY {hk},{cks_list_str} ORDER BY DV_APPTS DESC, DV_LOADTS DESC) AS DV_RNK
                        FROM {schema_edwh}.{sat} tgt        
                    )
                    INSERT INTO {schema_edwh}.{sat} SELECT DISTINCT
                    --attributes--
                    {attrs_val_list_str},
                    --cks--
                    {cks_val_list_str},
                    --hk--
                    {hk},
                    --meta--
                    DV_HASHDIFF,
                    current_timestamp,
                    DV_APPTS,
                    DV_RECSRC,
                    DV_TENANT,
                    DV_BKEYCODE,        
                    '{task_id}'
                    FROM {schema_stage}.{table_name}__{sat} src
                    WHERE NOT EXISTS ( 
                    SELECT 1 FROM CUR     
                        WHERE CUR.DV_RNK = 1
                        AND src.{hk} = CUR.{hk}
                        {sql_where_conds_ck_list_str}
                        AND src.DV_HASHDIFF = cur.DV_HASHDIFF
                    );
                    """
                ##------------------
                ## MULTI-ACTIVE SAT
                ##---------------------
                elif conf['sats'][sat]['multiactive']:
                    # cte's needed
                    sql = \
                    f"""WITH unique_staged_content as (
                    select DV_TENANT
                    , {hk}
                    , DV_RECSRC
                    , DV_TASKID
                    , DV_APPTS
                    , DV_LOADTS
                    , DV_HASHDIFF
                    , SUM( (ROWNR=1)::int) OVER (PARTITION BY {hk}) as DVCOUNT
                    , {attrs_val_list_str}
                    from 
                    ( 
                        select s.*
                    , ROW_NUMBER() OVER (PARTITION BY {hk},DV_HASHDIFF) as ROWNR
                    from {schema_stage}.{table_name}__{sat} s
                    ) drows
                    )
                    ,grp_cte as (
                    SELECT {hk}
                    , DV_APPTS
                    , DV_LOADTS 
                    , MAX(DV_SUBSEQ) as DVCOUNT
                    , RANK() OVER (PARTITION BY {hk} ORDER BY DV_APPTS DESC, DV_LOADTS DESC) as DVRANK
                    FROM {schema_edwh}.{sat}
                    GROUP BY {hk}, DV_APPTS, DV_LOADTS
                    )
                    INSERT INTO {schema_edwh}.{sat}
                    SELECT 
                    DV_TENANT
                    , {hk}
                    , DV_RECSRC
                    , '{task_id}' --task_id
                    , DV_APPTS
                    , DV_LOADTS
                    , DV_HASHDIFF
                    , DENSE_RANK() OVER (PARTITION BY {hk} ORDER BY DV_HASHDIFF) as DV_SUBSEQ
                    , {attrs_val_list_str}
                    FROM unique_staged_content dlt
                    WHERE EXISTS (
                        SELECT 1 FROM unique_staged_content stg 
                        WHERE NOT EXISTS (
                            SELECT 1 FROM (
                                SELECT
                                msat.{hk}
                                , msat.DV_HASHDIFF
                                , msat.DV_APPTS
                                , msat.DV_LOADTS
                                , msat.DV_SUBSEQ
                                , grp.DVCOUNT
                            FROM {schema_edwh}.{sat} msat
                            INNER JOIN (
                                SELECT * 
                                FROM grp_cte
                                WHERE DVRANK = 1
                            ) grp 
                            ON msat.{hk} = grp.{hk}
                            and msat.DV_APPTS = grp.DV_APPTS
                            and msat.DV_LOADTS = grp.DV_LOADTS
                        ) msat 
                        WHERE stg.{hk} = msat.{hk}
                        AND stg.DV_HASHDIFF = msat.DV_HASHDIFF
                        AND stg.DVCOUNT = msat.DVCOUNT
                    )
                    AND dlt.{hk} = stg.{hk}
                    );                
                    """        
                ##------------------
                ## REGULAR SAT
                ##------------------
                else:
                    sql=\
                    f"""INSERT INTO {schema_edwh}.{sat} SELECT DISTINCT
                    --attributes--
                    {attrs_val_list_str},
                    --hk--
                    {hk},
                    --meta--
                    DV_HASHDIFF,
                    current_timestamp,
                    DV_APPTS,
                    DV_RECSRC,
                    DV_TENANT,
                    DV_BKEYCODE,        
                    '{task_id}'
                    FROM {schema_stage}.{table_name}__{sat} src
                    WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {schema_edwh}.{sat} tgt
                    WHERE src.{hk} = tgt.{hk}
                    )
                    ;        
                    """        

                #sql=sqlparse.format(sql, reindent=True, keyword_case='upper')#,comma_first=True)
                self.sql += f"\n{sql}"
        
        
        self.doc=self.sql


    def execute(self, context: Context):        
        self.hook = PostgresHook(postgres_conn_id='pgconn')                                      
        self.hook.run(self.sql)

        
            
        