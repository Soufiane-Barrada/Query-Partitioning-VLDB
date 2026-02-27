SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", COUNT(DISTINCT "t"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE", AVG("lineitem"."l_quantity") AS "AVG_QUANTITY"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" <= DATE '1996-12-31') AS "t" INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name"