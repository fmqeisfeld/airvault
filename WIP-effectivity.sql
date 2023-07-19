drop table stage.test__es_test;
CREATE TABLE IF NOT EXISTS stage.test__es_test (
        --hk--
		hk_test_foo varchar(64) not null,
        hk_test varchar(64) NOT NULL,   
        hk_foo varchar(64) not null,
        --meta--
        DV_STARTTS timestamp null,
        DV_ENDTS timestamp null,
        DV_HASHDIFF varchar(64) NULL,
        DV_LOADTS timestamp NULL,
        DV_APPTS timestamp NULL,
        DV_RECSRC varchar(64) NULL,
        DV_TENANT varchar(64) NULL,
        DV_BKEYCODE varchar(64) NULL,
        DV_TASKID varchar(64) NULL        
        );
       
       

with current_effectivity as (
	select hk_test
		 , hk_foo
		 , dv_startts
		 , dv_endts
	from (
		select hk_test_foo
			 , dv_startts
			 , dv_endts
			 , rank() over (partition by hk_test_foo order by dv_appts desc, dv_loadts desc) as rnk
		from edwh.es_test
	) subq1
	join l_test_foo l
		on subq1.hk_test_foo = l.hk_test_foo
	where subq1.rnk=1 and subq1.dv_endts = '9999-12-31'
),
driverkey as (select distinct hk_test, dv_appts from quelle.test)