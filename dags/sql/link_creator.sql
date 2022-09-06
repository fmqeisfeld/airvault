CREATE TABLE {schema}.{link} (
	{hk} varchar(256) NOT NULL,
    load_dts timestamp NULL,     
    {bk_list}
    CONSTRAINT {link}_pkey PRIMARY KEY ({hk})
);