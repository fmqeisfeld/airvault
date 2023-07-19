INSERT INTO {{schema_edwh}}.{{link}} SELECT
--hk--
{{hk}},
--hks--        
{% for x in hks -%} 
{{x}},
{% endfor -%}
--cks--
{% for ck in cks -%} 
{{ck}},
{% endfor -%}
--meta--
current_timestamp,
DV_APPTS,
DV_RECSRC,
DV_TENANT,
DV_BKEYCODE,        
'{{task_id}}'
FROM {{schema_stage}}.{{table_name}}__{{link}} src
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{schema_edwh}}.{{link}} tgt
    WHERE src.{{hk}} = tgt.{{hk}}
);