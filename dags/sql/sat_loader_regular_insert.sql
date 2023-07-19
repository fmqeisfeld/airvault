INSERT INTO {{schema_edwh}}.{{sat}} SELECT DISTINCT
    --hk--
    {{hk}}            
    --attributes--            
    {% for attr in attrs -%}
    ,{{ attr }}
    {% endfor -%}            
    --meta--
    ,DV_HASHDIFF
    ,current_timestamp
    ,DV_APPTS
    ,DV_RECSRC
    ,DV_TENANT
    ,DV_BKEYCODE        
    ,'{{task_id}}'
    FROM {{schema_stage}}.{{table_name}}__{{sat}} src
        WHERE NOT EXISTS (
        SELECT 1 
        FROM {{schema_edwh}}.{{sat}} tgt
        WHERE src.{{hk}} = tgt.{{hk}}
    )
    ; 