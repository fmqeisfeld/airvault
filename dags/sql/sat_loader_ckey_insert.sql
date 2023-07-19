WITH CUR AS (
    SELECT {{hk}}                
        {% for ck in cks -%}
        , {{ck}}
        {% endfor -%}
        , DV_HASHDIFF
        , RANK() over (PARTITION BY {{hk}}
        {%- for ck in cks -%}
        , {{ck}} 
        {%- endfor %} 
        ORDER BY DV_APPTS DESC, DV_LOADTS DESC) AS DV_RNK
    FROM {{schema_edwh}}.{{sat}} tgt        
            )
INSERT INTO {{schema_edwh}}.{{sat}} 
SELECT DISTINCT
    --hk--
    {{hk}}      
    --cks--
    {% for ck in cks -%}
    ,{{ ck }}
    {% endfor -%}            
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
    SELECT 1 FROM CUR     
    WHERE CUR.DV_RNK = 1
    AND src.{{hk}} = CUR.{{hk}}
    {% for ck in cks -%}
    AND src.{{ck}}=cur.{{ck}}
    {% endfor -%}                
    AND src.DV_HASHDIFF = cur.DV_HASHDIFF
); 