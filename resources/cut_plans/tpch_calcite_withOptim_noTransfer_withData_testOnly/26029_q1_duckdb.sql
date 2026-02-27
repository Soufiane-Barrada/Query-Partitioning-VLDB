SELECT COALESCE("t"."p_name", "t"."p_name") AS "p_name", "supplier"."s_name", "customer"."c_name", ANY_VALUE("t"."p_name") AS "PRODUCT_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", COUNT(*) AS "TOTAL_ORDERS", SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) AS "TOTAL_SALES", AVG("t0"."l_quantity") AS "AVERAGE_QUANTITY", MAX("t0"."l_shipdate") AS "LAST_SHIP_DATE", LISTAGG(DISTINCT "region"."r_name", ', ') AS "REGIONS_SUPPLIED"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%metal%') AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN "TPCH"."orders" ON "t0"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t"."p_partkey" = "t0"."l_partkey"
GROUP BY "t"."p_name", "supplier"."s_name", "customer"."c_name"
HAVING SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) > 10000.0000