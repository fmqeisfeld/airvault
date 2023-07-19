WITH unique_staged_content as (            
    select 
        {{hk}}
        {% for attr in attrs -%}
        , {{attr}}
        {% endfor -%}            
        , DV_TENANT
        , DV_RECSRC
        , DV_TASKID
        , DV_APPTS
        , DV_LOADTS
        , DV_HASHDIFF
        , DV_BKEYCODE
        , SUM( (ROWNR=1)::int) OVER (PARTITION BY {{hk}}) as DVCOUNT
    from 
    ( 
        select s.*
             , ROW_NUMBER() OVER (PARTITION BY {{hk}},DV_HASHDIFF) as ROWNR
        from {{schema_stage}}.{{table_name}}__{{sat}} s
    ) drows )
,grp_cte as (
    SELECT {{hk}}
          , DV_APPTS
          , DV_LOADTS 
          , MAX(DV_SUBSEQ) as DVCOUNT
          , RANK() OVER (PARTITION BY {{hk}} ORDER BY DV_APPTS DESC, DV_LOADTS DESC) as DVRANK
    FROM {{schema_edwh}}.{{sat}}
    GROUP BY {{hk}}, DV_APPTS, DV_LOADTS
    )                
INSERT INTO {{schema_edwh}}.{{sat}}
SELECT {{hk}}
        {% for attr in attrs -%}
        ,{{ attr }}
        {% endfor -%}            
        , DV_HASHDIFF
        , DV_LOADTS    
        , DV_APPTS 
        , DV_RECSRC            
        , DV_TENANT
        , DV_BKEYCODE           
        , DENSE_RANK() OVER (PARTITION BY {{hk}} ORDER BY DV_HASHDIFF) as DV_SUBSEQ
        , '{{task_id}}'            
FROM unique_staged_content dlt
WHERE EXISTS (
    SELECT 1 FROM unique_staged_content stg 
    WHERE NOT EXISTS (
        SELECT 1 FROM (
            SELECT msat.{{hk}}
            , msat.DV_HASHDIFF
            , msat.DV_APPTS
            , msat.DV_LOADTS
            , msat.DV_SUBSEQ
            , grp.DVCOUNT
        FROM {{schema_edwh}}.{{sat}} msat
        INNER JOIN (
            SELECT * 
            FROM grp_cte
            WHERE DVRANK = 1
        ) grp 
        ON msat.{{hk}} = grp.{{hk}}
        and msat.DV_APPTS = grp.DV_APPTS
        and msat.DV_LOADTS = grp.DV_LOADTS
    ) msat 
    WHERE stg.{{hk}} = msat.{{hk}}
    AND stg.DV_HASHDIFF = msat.DV_HASHDIFF
    AND stg.DVCOUNT = msat.DVCOUNT
)
AND dlt.{{hk}} = stg.{{hk}}
);   