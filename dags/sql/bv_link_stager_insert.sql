INSERT INTO {{schema_stage}}.{{table_name}}__{{link}} SELECT 
--hk--
md5(concat_ws('|','{{tenant}}','{{bkeycode}}'
{% for bk in bks_list -%}
,coalesce(trim({{bk}}::varchar({{maxvarchar}})),'-1')
{% endfor -%}))
--hks--
{% for bk in bks_list -%}
,md5(coalesce(trim({{bk}}::varchar({{maxvarchar}})),'-1'))
{% endfor -%}
--cks--
{% for ck in cks -%}
, coalesce(trim({{ck}}::varchar({{maxvarchar}})),'-1')
{% endfor -%}
--meta--
,current_timestamp
,{{appts}}::timestamp
,'{{table_name}}'
,'{{tenant}}'
,'{{bkeycode}}'
,'{{task_id}}'
FROM ({{custom_sql}}) as custom_sql