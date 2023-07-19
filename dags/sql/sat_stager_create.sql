CREATE TABLE IF NOT EXISTS {{schema_stage}}.{{table_name}}__{{sat}} (
--hk--
{{hk}} varchar({{maxvarchar}}) NOT NULL   
--cks--
{% for ck in cks -%}
,{{ ck }} varchar({{maxvarchar}})
{% endfor -%}
--attributes--
{% for attr in attrs -%}
,{{ attr }} varchar({{maxvarchar}})
{% endfor -%}
--meta--
,DV_HASHDIFF varchar({{maxvarchar}}) NULL
,DV_LOADTS timestamp NULL
,DV_APPTS timestamp NULL
,DV_RECSRC varchar({{maxvarchar}}) NULL
,DV_TENANT varchar({{maxvarchar}}) NULL
,DV_BKEYCODE varchar({{maxvarchar}}) NULL
,DV_TASKID varchar({{maxvarchar}}) NULL        
);
TRUNCATE {{schema_stage}}.{{table_name}}__{{sat}};