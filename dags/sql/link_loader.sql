with cte (hk,load_dts,{bk_list}) as (
	select md5({bk_concat_str}) 
	,  current_timestamp 
	, {bk_list}
	from {schema_src}.{table_src} src
)
insert into {schema_tgt}.{table_tgt} ({hk_tgt},load_dts,{bk_list})
select distinct hk
  , load_dts
  , {bk_list}
from cte
where not exists 
(
	select 1 from {schema_tgt}.{table_tgt} tgt
	where cte.hk = tgt.{hk_tgt}
);