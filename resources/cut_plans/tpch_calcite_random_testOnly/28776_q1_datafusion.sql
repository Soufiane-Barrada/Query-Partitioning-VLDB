SELECT COALESCE("t"."p_name", "t"."p_name") AS "p_name", "supplier"."s_name", "customer"."c_name", ANY_VALUE("t"."p_name") AS "PART_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", COUNT(DISTINCT "t0"."o_orderkey") AS "TOTAL_ORDERS", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", AVG("lineitem"."l_tax") AS "AVERAGE_TAX", LISTAGG(DISTINCT "region"."r_name", ', ') AS "REGIONS_SUPPLIED"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey") ON "lineitem"."l_orderkey" = "t0"."o_orderkey") ON "t"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "t"."p_name", "supplier"."s_name", "customer"."c_name"