INSERT INTO {{schema_stage}}.{{table_name}}__{{hub}} SELECT
--hk--
md5(concat_ws('|','{{tenant}}','{{bkeycode}}'
{%- for bk in bks_src -%} 
,coalesce(trim({{bk}}::varchar({{maxvarchar}})),'-1')
{%- endfor %}))        
--bk--
,concat_ws('|'
{%- for bk in bks_src -%} 
,coalesce(trim({{bk}}::varchar({{maxvarchar}})),'-1')
{%- endfor %})
--meta--        
,current_timestamp
,{{appts}}::timestamp
,'{{table_name}}'
,'{{tenant}}'
,'{{bkeycode}}'
,'{{task_id}}'
FROM {{schema_source}}.{{table_name}}
;                        