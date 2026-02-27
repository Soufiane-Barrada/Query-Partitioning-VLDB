SELECT COALESCE("t5"."PART_NAME", "t5"."PART_NAME") AS "PART_NAME", "t5"."SUPPLIER_NAME", "t5"."CUSTOMER_NAME", "t5"."ORDER_DATE", "t5"."ORDER_COUNT", "t5"."TOTAL_REVENUE", "t5"."REGIONS_INVOLVED", "t5"."RETURN_STATUS"
FROM (SELECT ANY_VALUE("t1"."p_name") AS "PART_NAME", ANY_VALUE("s1"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("s1"."c_name") AS "CUSTOMER_NAME", ANY_VALUE("s1"."o_orderdate") AS "ORDER_DATE", COUNT(DISTINCT "s1"."l_orderkey") AS "ORDER_COUNT", ROUND(SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")), 2) AS "TOTAL_REVENUE", LISTAGG(DISTINCT "s1"."r_name", ', ') AS "REGIONS_INVOLVED", MAX(CASE WHEN "s1"."l_returnflag" = 'R' THEN 'Returned    ' ELSE 'Not Returned' END) AS "RETURN_STATUS"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%steel%') AS "t1"
INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "s1" ON "partsupp"."ps_partkey" = "s1"."l_partkey" AND "partsupp"."ps_suppkey" = "s1"."s_suppkey"
GROUP BY "t1"."p_name", "s1"."s_name", "s1"."c_name", "s1"."o_orderdate"
ORDER BY 6 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5"