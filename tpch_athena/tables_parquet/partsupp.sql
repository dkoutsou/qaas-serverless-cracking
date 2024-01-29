CREATE external TABLE partsupp
(
    ps_partkey     int,
    ps_suppkey     int,
    ps_availqty    int,
    ps_supplycost  double,
    ps_comment     varchar(199)
)
STORED AS PARQUET
