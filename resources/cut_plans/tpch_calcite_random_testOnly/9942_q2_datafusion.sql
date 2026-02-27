SELECT COALESCE("t8"."SUPPLIER_NAME", "t8"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t8"."CUSTOMER_NAME", "t8"."NATION_NAME", CAST("t8"."$f6" AS DECIMAL(19, 4)) AS "TOTAL_REVENUE", "t8"."TOTAL_ORDERS_COUNT"
FROM (SELECT "s1"."S_NAME", "customer"."c_name", "t4"."N_NAME", ANY_VALUE("s1"."S_NAME") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", ANY_VALUE("t4"."N_NAME") AS "NATION_NAME", SUM("t5"."l_extendedprice" * (1 - "t5"."l_discount")) AS "$f6", COUNT(DISTINCT "t3"."o_orderkey") AS "TOTAL_ORDERS_COUNT"
FROM "s1"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'F') AS "t3" INNER JOIN ((SELECT "nation"."n_nationkey" AS "N_NATIONKEY", "nation"."n_name" AS "N_NAME", "region"."r_name" AS "R_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey") AS "t4" INNER JOIN "TPCH"."customer" ON "t4"."N_NATIONKEY" = "customer"."c_nationkey") ON "t3"."o_custkey" = "customer"."c_custkey" INNER JOIN ("TPCH"."partsupp" AS "partsupp0" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t5" ON "partsupp0"."ps_partkey" = "t5"."l_partkey") ON "t3"."o_orderkey" = "t5"."l_orderkey") ON "s1"."S_SUPPKEY" = "partsupp0"."ps_suppkey"
GROUP BY "s1"."S_NAME", "customer"."c_name", "t4"."N_NAME"
ORDER BY 7 DESC NULLS FIRST) AS "t8"