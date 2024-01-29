CREATE external TABLE customer
(
    c_custkey     int,
    c_name        varchar(25),
    c_address     varchar(40),
    c_nationkey   int,
    c_phone       varchar(15),
    c_acctbal     double,
    c_mktsegment  varchar(10),
    c_comment     varchar(117)
)
STORED AS PARQUET
