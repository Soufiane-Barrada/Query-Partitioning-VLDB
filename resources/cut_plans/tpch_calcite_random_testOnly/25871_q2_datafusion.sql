SELECT COALESCE("SUPPLIER_NAME", "SUPPLIER_NAME") AS "SUPPLIER_NAME", "PART_NAME", "PART_BRAND", "TOTAL_AVAILABLE_QUANTITY", "AVG_EXTENDED_PRICE", "UNIQUE_CUSTOMERS", "REGION_NAME"
FROM (SELECT "SUPPLIER_NAME", "PART_NAME", "PART_BRAND", "TOTAL_AVAILABLE_QUANTITY", "AVG_EXTENDED_PRICE", "UNIQUE_CUSTOMERS", "REGION_NAME"
FROM (SELECT "t1"."s_name", "s1"."p_name", "s1"."p_brand", "region"."r_name", ANY_VALUE("t1"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("s1"."p_name") AS "PART_NAME", ANY_VALUE("s1"."p_brand") AS "PART_BRAND", SUM("s1"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("t2"."l_extendedprice") AS "AVG_EXTENDED_PRICE", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", ANY_VALUE("region"."r_name") AS "REGION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" LIKE '%reliable%') AS "t1" ON "nation"."n_nationkey" = "t1"."s_nationkey"
INNER JOIN "s1" ON "t1"."s_suppkey" = "s1"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t2" INNER JOIN "TPCH"."orders" ON "t2"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "s1"."p_partkey" = "t2"."l_partkey"
GROUP BY "region"."r_name", "t1"."s_name", "s1"."p_name", "s1"."p_brand") AS "t4"
WHERE "t4"."TOTAL_AVAILABLE_QUANTITY" > 50
ORDER BY "TOTAL_AVAILABLE_QUANTITY" DESC NULLS FIRST, "AVG_EXTENDED_PRICE") AS "t7"