SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "s_name", "t"."p_name", "region"."r_name", "supplier"."s_comment", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("t"."p_name") AS "PART_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", MIN("t0"."l_discount") AS "MIN_DISCOUNT", MAX("t0"."l_tax") AS "MAX_TAX", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE(TRIM(BOTH ' ' FROM "supplier"."s_comment")) AS "TRIMMED_SUPPLIER_COMMENT"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%metal%') AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN "TPCH"."orders" ON "t0"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t"."p_partkey" = "t0"."l_partkey"
GROUP BY "supplier"."s_name", "t"."p_name", "region"."r_name", "supplier"."s_comment"