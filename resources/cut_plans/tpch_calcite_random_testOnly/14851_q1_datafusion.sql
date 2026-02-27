SELECT COALESCE("t"."r_regionkey", "t"."r_regionkey") AS "r_regionkey", "t"."r_name", "t"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment", "t0"."l_orderkey", "t0"."l_partkey", "t0"."l_suppkey", "t0"."l_linenumber", "t0"."l_quantity", "t0"."l_extendedprice", "t0"."l_discount", "t0"."l_tax", "t0"."l_returnflag", "t0"."l_linestatus", "t0"."l_shipdate", "t0"."l_commitdate", "t0"."l_receiptdate", "t0"."l_shipinstruct", "t0"."l_shipmode", "t0"."l_comment"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1994-01-01' AND "l_shipdate" < DATE '1995-01-01') AS "t0" ON "supplier"."s_suppkey" = "t0"."l_suppkey"