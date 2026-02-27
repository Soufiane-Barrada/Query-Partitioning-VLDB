SELECT COALESCE("t5"."SUPPLIER_NAME", "t5"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t5"."PART_NAME", "t5"."TOTAL_AVAILABLE_QUANTITY", "t5"."MIN_DISCOUNT", "t5"."MAX_TAX", "t5"."UNIQUE_CUSTOMERS", "t5"."REGION_NAME", "t5"."TRIMMED_SUPPLIER_COMMENT"
FROM (SELECT "supplier"."s_name", "s1"."p_name", "region"."r_name", "supplier"."s_comment", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("s1"."p_name") AS "PART_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", MIN("t1"."l_discount") AS "MIN_DISCOUNT", MAX("t1"."l_tax") AS "MAX_TAX", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE(TRIM(BOTH ' ' FROM "supplier"."s_comment")) AS "TRIMMED_SUPPLIER_COMMENT"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" INNER JOIN "TPCH"."orders" ON "t1"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "s1"."p_partkey" = "t1"."l_partkey"
GROUP BY "supplier"."s_name", "s1"."p_name", "region"."r_name", "supplier"."s_comment"
HAVING SUM("partsupp"."ps_availqty") > 1000
ORDER BY 7 DESC NULLS FIRST, 10 DESC NULLS FIRST) AS "t5"