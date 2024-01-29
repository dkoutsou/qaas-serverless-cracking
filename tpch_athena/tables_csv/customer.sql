CREATE external TABLE customer
(
    c_custkey     int,
    c_name        varchar(25),
    c_address     varchar(40),
    c_nationkey   int,
    c_phone       varchar(15),
    c_acctbal     decimal(15,2),
    c_mktsegment  varchar(10),
    c_comment     varchar(117)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
