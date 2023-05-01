CREATE TABLE IF NOT EXISTS {{ schema_stage }}.{{ table_name }}__{{ sat_name }} (
        --hk--
        {{ hk }} varchar({{ maxvarchar }}) not null,
        --meta--
        DV_STARTTS timestamp null,
        DV_ENDTS timestamp null,
        DV_HASHDIFF varchar({{ maxvarchar }}) NULL,
        DV_LOADTS timestamp NULL,
        DV_APPTS timestamp NULL,
        DV_RECSRC varchar({{ maxvarchar }}) NULL,
        DV_TENANT varchar({{ maxvarchar }}) NULL,
        DV_BKEYCODE varchar({{ maxvarchar }}) NULL,
        DV_TASKID varchar({{ maxvarchar }}) NULL     
        );
       
       
with current_effectivity as (
	select subq.hk_test_foo
		 , l.{{ hk }}
         {% for k,v in bks.items() %} , l.{{ v }} {% endfor %}
		 , subq.dv_startts
		 , subq.dv_endts
	from (
		select {{ hk }}
			 , dv_startts
			 , dv_endts
			 , rank() over (partition by {{ hk }} order by dv_appts desc, dv_loadts desc) as rnk
		from {{ schema_edwh }}.{{ sat_name }}
	) subq
	join {{ link_name }} l
		on subq.{{ hk }} = l.{{ hk }}
	where subq.rnk=1 and subq.dv_endts = '9999-12-31'::timestamp
)
, driverkey as (
	select distinct md5(concat_ws('|',{{ tenant }} , {{ bkeycode }} {% for k in dks %}, coalesce(trim( {{k}}),'-1')) {%endfor%}) as dkey
			   	  , '{{ appts }}'::timestamp as DV_APPTS
	from {{ schema_source }}.{{ table_name }}
)
insert into stage.test__es_test_foo
-- generate open (high date) record
select MD5(CONCAT_WS('|',bk_test,bk_foo) as hk_test_foo
	 , DV_APPTS as DV_STARTTS
	 , '9999-12-31'::timestamp as DV_ENDTS
	 , MD5(CONCAT_WS('|',DV_APPTS::text,'9999-12-31') as DV_HASHDIFF
	 , 'somedate'::timestamp as DV_LOADTS
	 , DV_APPTS
	 , DV_RECSRC	
	 , DV_TENANT
	 , DV_BKEYCODE
	 , DV_TASKID
from quelle.test
where not exists (
	select 1
	from current_effectivity efs
	where stage.hk_test_foo = efs.hk_test_foo
)
union all 
-- generate closed record
select efs.hk_test_foo
	 , efs.dv_startts
	 , drv.APPTS as dv_endts
	 , MD5(CONCAT_WS('|',efs.DV_APPTS::text,drv.DV_ENDTS::text)
	 , 'somedate'::timestamp as DV_LOADTS
	 , drv.DV_APPTS
	 , DV_RECSRC
	 , DV_TENANT
	 , DV_BKEYCODE
	 , DV_TASKID
from current_effectivity efs
join driverkey drv
on efs.hk_test = drv.hk_test
where exists 
(
	select 1
	from quelle.test_foo
	where efs.hk_test = md5(concat_ws('|',bk_test)
)
and not exists 
(
	select 1
	from quelle.test_foo
	where efs.hk_test_foo = md5(concat_ws('|',bk_test,bk_foo)
)


