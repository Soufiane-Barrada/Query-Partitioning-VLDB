SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", "supplier"."s_name", "part"."p_comment", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", SUM("t"."l_quantity") AS "TOTAL_QUANTITY", SUM("t"."l_extendedprice") AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", MAX("t"."l_shipdate") AS "LAST_SHIP_DATE", MIN("t"."l_shipdate") AS "FIRST_SHIP_DATE", AVG("t"."l_discount") AS "AVERAGE_DISCOUNT", LISTAGG(DISTINCT "region"."r_name", ', ') AS "REGIONS_SUPPLY", ANY_VALUE(SUBSTRING("part"."p_comment", 1, 10)) AS "SHORT_COMMENT"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t" INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "part"."p_partkey" = "t"."l_partkey"
GROUP BY "part"."p_name", "supplier"."s_name", "part"."p_comment"
HAVING SUM("t"."l_quantity") > 100.00