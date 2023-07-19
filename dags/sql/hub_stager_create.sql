CREATE TABLE IF NOT EXISTS  {{schema_stage}}.{{table_name}}__{{hub}} (
--hk--
{{hk}} varchar({{maxvarchar}}) NOT NULL        
--bk--
,{{bk_tgt}} varchar({{maxvarchar}}) NOT NULL
--meta--        
,DV_LOADTS timestamp NULL
,DV_APPTS timestamp NULL
,DV_RECSRC varchar({{maxvarchar}}) NULL
,DV_TENANT varchar({{maxvarchar}}) NULL
,DV_BKEYCODE varchar({{maxvarchar}}) NULL
,DV_TASKID varchar({{maxvarchar}}) NULL      
);
TRUNCATE {{schema_stage}}.{{table_name}}__{{hub}};