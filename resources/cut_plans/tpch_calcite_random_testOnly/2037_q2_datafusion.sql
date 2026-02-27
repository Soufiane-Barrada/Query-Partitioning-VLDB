SELECT COALESCE("t24"."NATION_NAME", "t24"."NATION_NAME") AS "NATION_NAME", "t24"."TOTAL_SALES", "t24"."SUPPLIER_COUNT"
FROM (SELECT "s1"."NATION_NAME", CASE WHEN "s1"."FD_COL_2" IS NOT NULL THEN CAST("s1"."FD_COL_2" AS DECIMAL(19, 4)) ELSE 0.0000 END AS "TOTAL_SALES", "s1"."SUPPLIER_COUNT"
FROM (SELECT AVG(SUM("t18"."l_extendedprice" * (1 - "t18"."l_discount"))) AS "EXPR$0"
FROM "TPCH"."supplier" AS "supplier4"
INNER JOIN "TPCH"."partsupp" AS "partsupp1" ON "supplier4"."s_suppkey" = "partsupp1"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t17" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01') AS "t18" ON "t17"."o_orderkey" = "t18"."l_orderkey") ON "partsupp1"."ps_partkey" = "t18"."l_partkey"
GROUP BY "supplier4"."s_suppkey", "supplier4"."s_name") AS "t22"
INNER JOIN "s1" ON "t22"."EXPR$0" < "s1"."FD_COL_2"
ORDER BY 2 DESC NULLS FIRST) AS "t24"