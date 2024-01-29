-- TPC-H Query 6

select
        sum(l_extendedprice * l_discount) as revenue
from
        `PROJECT_NAME.DATASET_NAME.lineitem`
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1995-01-01'
        and l_discount between 0.05 and 0.07
        and l_quantity < 24