CREATE external TABLE lineitem (
    l_orderkey    int,
    l_partkey     int,
    l_suppkey     int,
    l_linenumber  int,
    l_quantity    int,
    l_extendedprice  double,
    l_discount    double,
    l_tax         double,
    l_returnflag  char(1),
    l_linestatus  char(1),
    l_shipdate    date,
    l_commitdate  date,
    l_receiptdate date,
    l_shipinstruct varchar(25),
    l_shipmode     varchar(10),
    l_comment      varchar(44)
)
STORED AS PARQUET
