CREATE external TABLE part
(
    p_partkey     int,
    p_name        varchar(55),
    p_mfgr        char(25),
    p_brand       varchar(10),
    p_type        varchar(25),
    p_size        int,
    p_container   varchar(10),
    p_retailprice decimal(15,2),
    p_comment     varchar(23)
)
STORED AS PARQUET
