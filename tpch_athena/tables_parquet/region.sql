CREATE external TABLE region
(
    r_regionkey  int,
    r_name       varchar(25),
    r_comment    varchar(152)
)
STORED AS PARQUET
