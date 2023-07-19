CREATE TABLE IF NOT EXISTS  {{schema_edwh}}.{{link}} (
--hk--
{{hk}} varchar({{maxvarchar}}) NOT NULL,
--hks--    
{% for x in hks -%} 
{{x}} varchar({{maxvarchar}}) NOT NULL,
{% endfor -%}
--cks--
{% for ck in cks -%} 
{{ck}} varchar({{maxvarchar}}) NOT NULL,
{% endfor -%}
--meta--    
DV_LOADTS timestamp NULL,
DV_APPTS timestamp NULL,
DV_RECSRC varchar({{maxvarchar}}) NULL,
DV_TENANT varchar({{maxvarchar}}) NULL,
DV_BKEYCODE varchar({{maxvarchar}}) NULL,
DV_TASKID varchar({{maxvarchar}}) NULL        
);