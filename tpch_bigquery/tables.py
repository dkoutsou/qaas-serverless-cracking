from google.cloud import bigquery


lineitem_schema = [
        bigquery.SchemaField("l_orderkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("l_partkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("l_suppkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("l_linenumber", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("l_quantity", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("l_extendedprice", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("l_discount", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("l_tax", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("l_returnflag", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("l_linestatus", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("l_shipdate", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("l_commitdate", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("l_receiptdate", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("l_shipinstruct", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("l_shipmode", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("l_comment", "STRING", mode="REQUIRED"),
        ]

customer_schema = [
        bigquery.SchemaField("c_custkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("c_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("c_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("c_nationkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("c_phone", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("c_acctbal", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("c_mktsegment", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("c_comment", "STRING", mode="REQUIRED"),
        ]

nation_schema = [
        bigquery.SchemaField("n_nationkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("n_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("n_regionkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("n_comment", "STRING", mode="REQUIRED"),
        ]

orders_schema = [
        bigquery.SchemaField("o_orderkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("o_custkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("o_orderstatus", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("o_totalprice", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("o_orderdate", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("o_orderpriority", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("o_clerk", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("o_shippriority", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("o_comment", "STRING", mode="REQUIRED"),
        ]

part_schema = [
        bigquery.SchemaField("p_partkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("p_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_mfgr", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_brand", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_size", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("p_container", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_retailprice", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("p_comment", "STRING", mode="REQUIRED"),
        ]

partsupp_schema = [
        bigquery.SchemaField("ps_partkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ps_suppkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ps_availqty", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ps_supplycost", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("ps_comment", "STRING", mode="REQUIRED"),
        ]

supplier_schema = [
        bigquery.SchemaField("s_suppkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_nationkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_phone", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_acctbal", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("s_comment", "STRING", mode="REQUIRED"),
        ]

region_schema = [
        bigquery.SchemaField("r_regionkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("r_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("r_comment", "STRING", mode="REQUIRED"),
        ]
