SELECT COALESCE(ANY_VALUE(CONCAT("customer"."c_name", ' (', "nation"."n_name", ')')), ANY_VALUE(CONCAT("customer"."c_name", ' (', "nation"."n_name", ')'))) AS "CUSTOMER_INFO", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", LISTAGG(DISTINCT CONCAT("part"."p_name", ' [', CAST("partsupp"."ps_supplycost" AS VARCHAR CHARACTER SET "ISO-8859-1"), ']'), ', ') AS "SUPPLIED_PARTS", MAX("orders"."o_orderdate") AS "LAST_ORDER_DATE"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN "TPCH"."partsupp" ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
WHERE "lineitem"."l_shipdate" >= '1997-01-01'
GROUP BY "customer"."c_custkey", "customer"."c_name", "nation"."n_name"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 10000.0000