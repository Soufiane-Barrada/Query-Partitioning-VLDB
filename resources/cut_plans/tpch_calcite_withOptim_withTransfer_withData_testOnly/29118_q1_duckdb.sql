SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "p_name", "region"."r_name", "nation"."n_name", "supplier"."s_name", "customer"."c_name", SUM("t"."l_quantity") AS "TOTAL_QUANTITY", AVG("t"."l_extendedprice") AS "AVG_PRICE", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE("nation"."n_name") AS "NATION_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" <= DATE '1996-12-31') AS "t" ON "customer"."c_custkey" = "t"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%widget%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t"."l_partkey" = "t0"."p_partkey"
GROUP BY "customer"."c_name", "region"."r_name", "nation"."n_name", "supplier"."s_name", "t0"."p_name"