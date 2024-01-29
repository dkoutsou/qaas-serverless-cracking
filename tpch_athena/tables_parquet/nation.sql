CREATE external TABLE nation
(
    n_nationkey  int,
    n_name       varchar(25),
    n_regionkey  int,
    n_comment    varchar(152)
)
STORED AS PARQUET
