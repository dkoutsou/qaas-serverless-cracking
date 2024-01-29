CREATE external TABLE lineitem (
    l_orderkey    int,
    l_partkey     int,
    l_suppkey     int,
    l_linenumber  int,
    l_quantity    decimal(15, 2),
    l_extendedprice  decimal(15, 2),
    l_discount    decimal(15, 2),
    l_tax         decimal(15, 2),
    l_returnflag  char(1),
    l_linestatus  char(1),
    l_shipdate    date,
    l_commitdate  date,
    l_receiptdate date,
    l_shipinstruct varchar(25),
    l_shipmode     varchar(10),
    l_comment      varchar(44)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
