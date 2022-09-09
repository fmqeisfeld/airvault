with cte (hk,
		 {hk_list_keys},
		 {bk_list_keys},
		 DV_LOADTS, 
		 DV_APPTS, 
		 DV_RECSRC, 
		 DV_TENANT, 
		 DV_BKEYCODE, 
		 DV_TASKID
	) as (
	select md5({concat_str}) 
	, {hk_list_vals}
	, {bk_list_vals}
	,  current_timestamp 
	, {appts}
	, '{rec_src}'
	, '{tenant}'
	, '{bkeycode}'
	, '{taskid}	'
	from {schema_src}.{table_src} src
)
insert into {schema_tgt}.{table_tgt} (
	{hk_tgt},
    {hk_list_keys},
    {bk_list_keys},
	DV_LOADTS, 
	DV_APPTS, 
	DV_RECSRC, 
	DV_TENANT, 
	DV_BKEYCODE, 
	DV_TASKID	
)
select distinct hk,
    {hk_list_keys},
    {bk_list_keys},
	DV_LOADTS, 
	DV_APPTS, 
	DV_RECSRC, 
	DV_TENANT, 
	DV_BKEYCODE, 
	DV_TASKID	
from cte
where not exists 
(
	select 1 from {schema_tgt}.{table_tgt} tgt
	where cte.hk = tgt.{hk_tgt}
);