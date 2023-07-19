INSERT INTO {{schema_stage}}.{{table_name}}__{{sat}} SELECT
--hk--
md5(concat_ws('|','{{tenant}}','{{bkeycode}}'
{% for bk in bks -%}
,coalesce(trim({{bk}}::varchar({{maxvarchar}})),'-1')
{% endfor -%}
))
--cks--
{% for ck in cks -%}
,{{ck}}
{% endfor -%}
--attributes--
{% for attr in attrs -%}
, {{attr}}
{% endfor -%}
--meta--
,md5(concat_ws('|'
{%- for attr in attrs -%}
,trim({{attr}}::varchar({{maxvarchar}}))
{%- endfor -%}
))
,current_timestamp
,{{appts}}::timestamp
,'{{table_name}}'
,'{{tenant}}'
,'{{bkeycode}}'
,'{{task_id}}'
FROM ({{custom_sql}}) as custom_sql
;        