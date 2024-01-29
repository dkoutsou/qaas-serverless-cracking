select
        s_name,
        s_address
from
        `PROJECT_NAME.DATASET_NAME.supplier`,
        `PROJECT_NAME.DATASET_NAME.nation`
where
        s_suppkey in (
                select
                        ps_suppkey
                from
                        `PROJECT_NAME.DATASET_NAME.partsupp`
                where
                        ps_partkey in (
                                select
                                        p_partkey
                                from
                                        `PROJECT_NAME.DATASET_NAME.part`
                                where
                                        p_name like 'forest%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        `PROJECT_NAME.DATASET_NAME.lineitem`
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1994-01-01'
                                        and l_shipdate < date '1995-01-01'
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
order by
        s_name
