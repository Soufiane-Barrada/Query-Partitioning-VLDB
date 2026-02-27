SELECT COALESCE("t8"."SUPPLIER_NAME", "t8"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t8"."REGION", "t8"."NATION", "t8"."TOTAL_ORDERS"
FROM (SELECT "supplier0"."s_suppkey", "supplier0"."s_name", "region"."r_name", "nation"."n_name", ANY_VALUE("supplier0"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("region"."r_name") AS "REGION", ANY_VALUE("nation"."n_name") AS "NATION", SUM(CASE WHEN "t2"."ORDER_COUNT" IS NOT NULL THEN CAST("t2"."ORDER_COUNT" AS BIGINT) ELSE 0 END) AS "TOTAL_ORDERS", SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")) AS "$f8"
FROM (SELECT "customer"."c_custkey", COUNT("orders"."o_orderkey") AS "ORDER_COUNT"
FROM "TPCH"."orders"
RIGHT JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
GROUP BY "customer"."c_custkey") AS "t2"
RIGHT JOIN ("TPCH"."customer" AS "customer0" INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t3" INNER JOIN ("TPCH"."lineitem" AS "lineitem0" INNER JOIN ("TPCH"."partsupp" AS "partsupp0" INNER JOIN ("TPCH"."nation" INNER JOIN ("TPCH"."supplier" AS "supplier0" INNER JOIN (SELECT "S_SUPPKEY", "TOTAL_REVENUE"
FROM "s1"
ORDER BY "TOTAL_REVENUE" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4" ON "supplier0"."s_suppkey" = "t4"."S_SUPPKEY") ON "nation"."n_nationkey" = "supplier0"."s_nationkey" INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey") ON "partsupp0"."ps_suppkey" = "supplier0"."s_suppkey") ON "lineitem0"."l_partkey" = "partsupp0"."ps_partkey") ON "t3"."o_orderkey" = "lineitem0"."l_orderkey") ON "customer0"."c_custkey" = "t3"."o_custkey") ON "t2"."c_custkey" = "customer0"."c_custkey"
GROUP BY "supplier0"."s_suppkey", "supplier0"."s_name", "region"."r_name", "nation"."n_name"
HAVING SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")) > 100000.0000
ORDER BY 8 DESC NULLS FIRST) AS "t8"