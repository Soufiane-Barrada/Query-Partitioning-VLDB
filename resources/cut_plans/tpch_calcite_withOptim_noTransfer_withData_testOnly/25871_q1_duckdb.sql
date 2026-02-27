SELECT COALESCE("s_name", "s_name") AS "s_name", "p_name", "p_brand", "r_name", "SUPPLIER_NAME", "PART_NAME", "PART_BRAND", "TOTAL_AVAILABLE_QUANTITY", "AVG_EXTENDED_PRICE", "UNIQUE_CUSTOMERS", "REGION_NAME"
FROM (SELECT "t"."s_name", "t0"."p_name", "t0"."p_brand", "region"."r_name", ANY_VALUE("t"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("t0"."p_name") AS "PART_NAME", ANY_VALUE("t0"."p_brand") AS "PART_BRAND", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("t1"."l_extendedprice") AS "AVG_EXTENDED_PRICE", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", ANY_VALUE("region"."r_name") AS "REGION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" LIKE '%reliable%') AS "t" ON "nation"."n_nationkey" = "t"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%metal%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" INNER JOIN "TPCH"."orders" ON "t1"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t0"."p_partkey" = "t1"."l_partkey"
GROUP BY "region"."r_name", "t"."s_name", "t0"."p_name", "t0"."p_brand") AS "t3"
WHERE "t3"."TOTAL_AVAILABLE_QUANTITY" > 50