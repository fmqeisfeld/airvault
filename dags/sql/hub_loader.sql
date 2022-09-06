with cte (hk,load_dts,rec_src,bk) as (
	select md5(trim(src.{bk_src}::varchar(256))) 
	,  current_timestamp 
	,  '{rec_src}'
	, src.{bk_src}
	from {schema_src}.{table_src} src
)
insert into {schema_tgt}.{table_tgt} ({hk_tgt},load_dts,rec_src,{bk_tgt})
select distinct 
	hk
  , load_dts
  , rec_src
  , bk
from cte
where not exists 
(
	select 1 from {schema_tgt}.{table_tgt} tgt
	where cte.hk = tgt.{hk_tgt}
);