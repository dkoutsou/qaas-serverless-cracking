select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
                select
                        substring(c_phone, 0, 2) as cntrycode,
                        c_acctbal
                from
                        `PROJECT_NAME.DATASET_NAME.customer`
                where
                        substring(c_phone, 0, 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                                select
                                        avg(c_acctbal)
                                from
                                        `PROJECT_NAME.DATASET_NAME.customer`
                                where
                                        c_acctbal > 0.00
                                        and substring(c_phone, 0, 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        `PROJECT_NAME.DATASET_NAME.orders`
                                where
                                        o_custkey = c_custkey
                        )
        ) as custsale
group by
        cntrycode
order by
        cntrycode
