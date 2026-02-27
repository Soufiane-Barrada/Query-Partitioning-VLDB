SELECT COALESCE("t3"."SUPPLIER_NAME", "t3"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t3"."PART_NAME", "t3"."DESCRIPTION", "t3"."REGION_NAME", "t3"."TOTAL_ORDERS", "t3"."TOTAL_QUANTITY", "t3"."TOTAL_REVENUE", "t3"."AVG_PRICE_AFTER_DISCOUNT"
FROM (SELECT "supplier"."s_name", "part"."p_name", "s1"."r_name", "partsupp"."ps_supplycost", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("part"."p_name") AS "PART_NAME", ANY_VALUE(CONCAT('Supplier ', "supplier"."s_name", ' supplies ', "part"."p_name", ' with a price of ', CAST("partsupp"."ps_supplycost" AS VARCHAR CHARACTER SET "ISO-8859-1"), ' per unit.')) AS "DESCRIPTION", ANY_VALUE("s1"."r_name") AS "REGION_NAME", COUNT(DISTINCT "s1"."o_orderkey") AS "TOTAL_ORDERS", SUM("s1"."l_quantity") AS "TOTAL_QUANTITY", SUM("s1"."l_extendedprice") AS "TOTAL_REVENUE", AVG(CASE WHEN "s1"."l_discount" > 0.00 THEN "s1"."l_extendedprice" * (1 - "s1"."l_discount") ELSE CAST("s1"."l_extendedprice" AS DECIMAL(19, 4)) END) AS "AVG_PRICE_AFTER_DISCOUNT"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "s1" ON "part"."p_partkey" = "s1"."l_partkey"
GROUP BY "supplier"."s_name", "part"."p_name", "s1"."r_name", "partsupp"."ps_supplycost"
ORDER BY 11 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"