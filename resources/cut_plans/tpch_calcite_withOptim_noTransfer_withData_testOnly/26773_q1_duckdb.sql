SELECT COALESCE("customer"."c_custkey", "customer"."c_custkey") AS "c_custkey", "customer"."c_name", "nation"."n_name", ANY_VALUE(CONCAT("customer"."c_name", ' (', "nation"."n_name", ')')) AS "CUSTOMER_INFO", SUM("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", LISTAGG(DISTINCT CONCAT("part"."p_name", ' [', CAST("partsupp"."ps_supplycost" AS VARCHAR CHARACTER SET "ISO-8859-1"), ']'), ', ') AS "SUPPLIED_PARTS", MAX("orders"."o_orderdate") AS "LAST_ORDER_DATE"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01') AS "t" ON "orders"."o_orderkey" = "t"."l_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "partsupp"."ps_partkey" = "t"."l_partkey" AND "partsupp"."ps_suppkey" = "t"."l_suppkey"
GROUP BY "customer"."c_custkey", "customer"."c_name", "nation"."n_name"