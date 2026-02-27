SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", ANY_VALUE("nation"."n_name") AS "NATION", SUM("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "TOTAL_REVENUE", AVG("supplier"."s_acctbal") AS "AVG_SUPPLIER_BALANCE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", COUNT(DISTINCT "customer"."c_custkey") AS "TOTAL_CUSTOMERS"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey"
INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t" INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey") ON "partsupp"."ps_partkey" = "t"."l_partkey") ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "nation"."n_name"
HAVING SUM("t"."l_extendedprice" * (1 - "t"."l_discount")) > 500000.0000