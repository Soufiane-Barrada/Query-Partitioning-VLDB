SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "P_NAME", "t0"."s_name" AS "S_NAME", "region"."r_name", "nation"."n_name", CONCAT('Region: ', "region"."r_name", ', Nation: ', "nation"."n_name", ', Supplier: ', "t0"."s_name") AS "FD_COL_4", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_5"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" NOT LIKE '%fraud%') AS "t0" ON "nation"."n_nationkey" = "t0"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE 'prod_%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t1"."p_partkey"