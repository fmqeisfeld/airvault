INSERT INTO {{schema_stage}}.{{table_name}}__{{link}} SELECT 
--hk--
md5(concat_ws('|', '{{tenant}}','{{bkeycode}}'
{%- for key in hks -%}
{%- for x in bks_mapping_inv[key] %}
,coalesce(trim({{x}}::varchar({{maxvarchar}})),'-1')
{%- endfor %}
{%- endfor %}
))
--hks--        
{%- for key in hks -%}
{%- for x in bks_mapping_inv[key] %}
,md5(concat_ws('|', '{{tenant}}','{{bkeycode}}',coalesce(trim({{x}}::varchar({{maxvarchar}})),'-1')))
{%- endfor %}
{%- endfor %}        
--cks--
{% for ck in cks -%}
{% if ck in records -%}
,coalesce(trim({{ck}}::varchar({{maxvarchar}})),'-1')
{% else -%}
,'-1'
{% endif -%}
{% endfor -%}
--meta--
,current_timestamp
,{{appts}}::timestamp
,'{{table_name}}'
,'{{tenant}}'
,'{{bkeycode}}'
,'{{task_id}}'
FROM {{schema_source}}.{{table_name}};