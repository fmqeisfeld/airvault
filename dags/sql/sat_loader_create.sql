CREATE TABLE IF NOT EXISTS {{schema_edwh}}.{{sat}} (
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
{% if multiactive -%}
,DV_SUBSEQ int NOT NULL
{% endif -%}
,DV_TASKID varchar({{maxvarchar}}) NULL
);