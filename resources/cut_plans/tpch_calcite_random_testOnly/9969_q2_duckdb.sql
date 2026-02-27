SELECT COALESCE("t4"."NATION_NAME", "t4"."NATION_NAME") AS "NATION_NAME", "t4"."TOTAL_REVENUE", "t4"."REGION_NAME"
FROM (SELECT "s1"."n_name", "s1"."r_name", ANY_VALUE("s1"."n_name") AS "NATION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("s1"."r_name") AS "REGION_NAME"
FROM "TPCH"."lineitem"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1998-01-01') AS "t0" INNER JOIN "s1" ON "t0"."o_custkey" = "s1"."c_custkey") ON "lineitem"."l_orderkey" = "t0"."o_orderkey"
GROUP BY "s1"."n_name", "s1"."r_name"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 100000.0000
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"