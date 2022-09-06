CREATE TABLE {schema}.{hub} (
	{hk} varchar(256) NOT NULL,
	load_dts timestamp NULL,
	rec_src varchar(256) NULL,
	{bk} {bk_type} NULL,
	CONSTRAINT {hub}_pkey PRIMARY KEY ({hk})
);