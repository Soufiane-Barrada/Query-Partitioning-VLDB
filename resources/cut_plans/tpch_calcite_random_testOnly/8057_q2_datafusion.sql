SELECT COALESCE("t4"."NATION", "t4"."NATION") AS "NATION", "t4"."REGION", "t4"."TOTAL_REVENUE", "t4"."TOTAL_ORDERS", "t4"."UNIQUE_CUSTOMERS", "t4"."AVERAGE_ORDER_VALUE"
FROM (SELECT "nation"."n_name", "region"."r_name", ANY_VALUE("nation"."n_name") AS "NATION", ANY_VALUE("region"."r_name") AS "REGION", SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "t1"."o_orderkey") AS "TOTAL_ORDERS", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", AVG("t1"."o_totalprice") AS "AVERAGE_ORDER_VALUE"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'TRUCK')) AS "t0" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey") ON "t0"."l_orderkey" = "t1"."o_orderkey") ON "s1"."p_partkey" = "t0"."l_partkey"
GROUP BY "nation"."n_name", "region"."r_name"
ORDER BY 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"