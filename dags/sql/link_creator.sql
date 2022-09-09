CREATE TABLE {schema}.{link} (
	{hk} varchar(256) NOT NULL,
	{hk_list_create},
	{bk_list_create},
	-- technical fields
	DV_LOADTS timestamp NULL,
	DV_APPTS timestamp NULL,
	DV_RECSRC varchar(256) NULL,
	DV_TENANT varchar(256) NULL,
	DV_BKEYCODE varchar(256) NULL,
	DV_TASKID varchar(256) NULL,
    CONSTRAINT {link}_pkey PRIMARY KEY ({hk})
);