INSERT INTO {{schema_stage}}.{{table_name}}__{{sat}} SELECT
--hk--
md5(concat_ws('|','{{tenant}}','{{bkeycode}}'
{% for bk in bks -%}
,coalesce(trim({{bk}}::varchar({{maxvarchar}})),'-1')
{% endfor -%}
))
--cks--
{% for ck in cks -%}
{% if ck in src_sat_cols[table_name] -%}
,coalesce(trim({{ck}}::varchar({{maxvarchar}})),'-1')
{% else -%}
,'-1'
{% endif -%}
{% endfor -%}
--attributes--
{% for attr in attrs -%}
,{{attr}}
{% endfor -%}          
--meta--
,md5(concat_ws('|'
{%- for attr in attrs -%}
,{{attr}}
{%- endfor -%}
))              
,current_timestamp
,{{appts}}::timestamp
,'{{table_name}}'
,'{{tenant}}'
,'{{bkeycode}}'
,'{{task_id}}'
FROM {{schema_source}}.{{table_name}};