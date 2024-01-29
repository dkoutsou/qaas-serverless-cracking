select
        o_orderpriority,
        count(*) as order_count
from
        `PROJECT_NAME.DATASET_NAME.orders`
where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1993-10-01'
        and exists (
                select
                        *
                from
                        `PROJECT_NAME.DATASET_NAME.lineitem`
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
group by
        o_orderpriority
order by
        o_orderpriority
