CREATE external TABLE orders
(
    o_orderkey       int,
    o_custkey        int,
    o_orderstatus    char(1),
    o_totalprice     double,
    o_orderdate      date,
    o_orderpriority  varchar(15),
    o_clerk          char(15),
    o_shippriority   int,
    o_comment        varchar(79)
)
STORED AS PARQUET
