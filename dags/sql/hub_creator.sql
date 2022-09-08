CREATE TABLE {schema}.{hub} (
	{hk} varchar(256) NOT NULL,
	{bk} varchar(256) NULL,
	-- technical fields
	DV_LOADTS timestamp NULL,
	DV_APPTS timestamp NULL,
	DV_RECSRC varchar(256) NULL,
	DV_TENANT varchar(256) NULL,
	DV_BKEYCODE varchar(256) NULL,
	DV_TASKID varchar(256) NULL,
	CONSTRAINT {hub}_pkey PRIMARY KEY ({hk})
);