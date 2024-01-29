CREATE external TABLE supplier
(
    s_suppkey     int,
    s_name        char(25),
    s_address     varchar(40),
    s_nationkey   int,
    s_phone       char(15),
    s_acctbal     decimal(15,2),
    s_comment     varchar(101)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
