SELECT COALESCE("t3"."REGION_NAME", "t3"."REGION_NAME") AS "REGION_NAME", "t3"."TOTAL_SALES", "t3"."UNIQUE_CUSTOMERS", "t3"."ORDER_COUNT", "t3"."TOTAL_SALES" / CASE WHEN "t3"."UNIQUE_CUSTOMERS" = 0 THEN NULL ELSE "t3"."UNIQUE_CUSTOMERS" END AS "AVG_SALES_PER_CUSTOMER", "t3"."TOTAL_SALES" / CASE WHEN "t3"."ORDER_COUNT" = 0 THEN NULL ELSE "t3"."ORDER_COUNT" END AS "AVG_SALES_PER_ORDER"
FROM (SELECT "s1"."r_regionkey" AS "R_REGIONKEY", "s1"."r_name", ANY_VALUE("s1"."r_name") AS "REGION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_SALES", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", COUNT(DISTINCT "t0"."o_orderkey") AS "ORDER_COUNT"
FROM "s1"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-10-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey") ON "lineitem"."l_orderkey" = "t0"."o_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "s1"."r_regionkey", "s1"."r_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"