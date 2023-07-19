INSERT INTO {{schema_edwh}}.{{hub}} SELECT        
--hk--
{{hk}}        
--bk--
,{{bk_tgt}}
--meta--
,current_timestamp
,DV_APPTS
,DV_RECSRC
,DV_TENANT
,DV_BKEYCODE
,'{{task_id}}'
FROM {{schema_stage}}.{{table_name}}__{{hub}} src
WHERE NOT EXISTS (
SELECT 1 
FROM {{schema_edwh}}.{{hub}} tgt
WHERE src.{{hk}} = tgt.{{hk}}
)
; 