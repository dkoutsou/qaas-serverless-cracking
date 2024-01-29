CREATE external TABLE partsupp
(
    ps_partkey     int,
    ps_suppkey     int,
    ps_availqty    int,
    ps_supplycost  decimal(15,2),
    ps_comment     varchar(199)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
