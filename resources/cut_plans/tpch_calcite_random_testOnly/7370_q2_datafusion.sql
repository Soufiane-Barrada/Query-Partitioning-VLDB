SELECT COALESCE("t4"."NATION_NAME", "t4"."NATION_NAME") AS "NATION_NAME", "t4"."REGION_NAME", "t4"."TOTAL_REVENUE", "t4"."NUMBER_OF_ORDERS", "t4"."UNIQUE_CUSTOMERS"
FROM (SELECT "s1"."n_name", "s1"."r_name", ANY_VALUE("s1"."n_name") AS "NATION_NAME", ANY_VALUE("s1"."r_name") AS "REGION_NAME", SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "t1"."o_orderkey") AS "NUMBER_OF_ORDERS", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'TRUCK')) AS "t0" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-10-01') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey") ON "t0"."l_orderkey" = "t1"."o_orderkey") ON "part"."p_partkey" = "t0"."l_partkey"
GROUP BY "s1"."n_name", "s1"."r_name"
ORDER BY 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"