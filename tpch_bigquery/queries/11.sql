select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
from
        `PROJECT_NAME.DATASET_NAME.partsupp`,
        `PROJECT_NAME.DATASET_NAME.supplier`,
        `PROJECT_NAME.DATASET_NAME.nation`
where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
group by
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                        select
                                sum(ps_supplycost * ps_availqty) * 0.0001
                        from
                                `PROJECT_NAME.DATASET_NAME.partsupp`,
                                `PROJECT_NAME.DATASET_NAME.supplier`,
                                `PROJECT_NAME.DATASET_NAME.nation`
                        where
                                ps_suppkey = s_suppkey
                                and s_nationkey = n_nationkey
                                and n_name = 'GERMANY'
                )
order by
        value desc
