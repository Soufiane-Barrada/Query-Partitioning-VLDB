SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "s_suppkey", "supplier"."s_name", "region"."r_name", "nation"."n_name", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("region"."r_name") AS "REGION", ANY_VALUE("nation"."n_name") AS "NATION", SUM(CASE WHEN "t"."ORDER_COUNT" IS NOT NULL THEN CAST("t"."ORDER_COUNT" AS BIGINT) ELSE 0 END) AS "TOTAL_ORDERS", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "FD_COL_8"
FROM (SELECT "customer"."c_custkey", COUNT("orders"."o_orderkey") AS "ORDER_COUNT"
FROM "TPCH"."orders"
RIGHT JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
GROUP BY "customer"."c_custkey") AS "t"
RIGHT JOIN ("TPCH"."customer" AS "customer0" INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t0" INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."partsupp" INNER JOIN ("TPCH"."nation" INNER JOIN ("TPCH"."supplier" INNER JOIN (SELECT "supplier0"."s_suppkey" AS "S_SUPPKEY", SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."lineitem" AS "lineitem0"
INNER JOIN ("TPCH"."supplier" AS "supplier0" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey") ON "lineitem0"."l_partkey" = "partsupp0"."ps_partkey"
GROUP BY "supplier0"."s_suppkey"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3" ON "supplier"."s_suppkey" = "t3"."S_SUPPKEY") ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey") ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey") ON "t0"."o_orderkey" = "lineitem"."l_orderkey") ON "customer0"."c_custkey" = "t0"."o_custkey") ON "t"."c_custkey" = "customer0"."c_custkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name", "region"."r_name", "nation"."n_name"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 100000.0000