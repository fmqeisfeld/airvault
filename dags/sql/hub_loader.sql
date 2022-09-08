with cte (hk,bk,dv_loadts,dv_appts,dv_recsrc,dv_tenant,dv_bkeycode,dv_taskid) as (
	select md5(concat_ws('|',
			   '{tenant}','{bkeycode}',
			   trim(src.{bk_src}::varchar(256))
	))
	, trim(src.{bk_src}::varchar(256))
	, current_timestamp 
	, {appts}
	, '{rec_src}'
	, '{tenant}'
	, '{bkeycode}'
	, '{taskid}	'
	from {schema_src}.{table_src} src
)
insert into {schema_tgt}.{table_tgt} (
		{hk_tgt}
	  , {bk_tgt}
	  , dv_loadts
	  , dv_appts
	  , dv_recsrc
	  , dv_tenant
	  , dv_bkeycode
	  , dv_taskid
	)
select distinct 
	hk
  , bk
  , dv_loadts
  , dv_appts
  , dv_recsrc
  , dv_tenant
  , dv_bkeycode
  , dv_taskid
from cte
where not exists 
(
	select 1 from {schema_tgt}.{table_tgt} tgt
	where cte.hk = tgt.{hk_tgt}
);