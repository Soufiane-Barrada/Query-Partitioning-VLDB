SELECT COALESCE("t5"."PART_NAME", "t5"."PART_NAME") AS "PART_NAME", "t5"."SUPPLIER_NAME", "t5"."CUSTOMER_NAME", "t5"."ORDER_ID", "t5"."LINE_ITEM_COUNT", "t5"."TOTAL_EXTENDED_PRICE", "t5"."AVERAGE_DISCOUNT", "t5"."LATEST_SHIP_DATE", "t5"."REGIONS_SUPPLIED", "t5"."PART_COMMENTS"
FROM (SELECT "t1"."p_name", "s1"."s_name", "customer"."c_name", "t2"."o_orderkey", ANY_VALUE("t1"."p_name") AS "PART_NAME", ANY_VALUE("s1"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", ANY_VALUE("t2"."o_orderkey") AS "ORDER_ID", COUNT(*) AS "LINE_ITEM_COUNT", SUM("lineitem"."l_extendedprice") AS "TOTAL_EXTENDED_PRICE", AVG("lineitem"."l_discount") AS "AVERAGE_DISCOUNT", MAX("lineitem"."l_shipdate") AS "LATEST_SHIP_DATE", LISTAGG(DISTINCT "s1"."r_name", ', ') AS "REGIONS_SUPPLIED", LISTAGG(DISTINCT "t1"."p_comment", '|') AS "PART_COMMENTS"
FROM "s1"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderstatus" = 'O') AS "t2" INNER JOIN "TPCH"."customer" ON "t2"."o_custkey" = "customer"."c_custkey") ON "lineitem"."l_orderkey" = "t2"."o_orderkey") ON "t1"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "t1"."p_name", "s1"."s_name", "customer"."c_name", "t2"."o_orderkey"
ORDER BY 10 DESC NULLS FIRST, 9
FETCH NEXT 50 ROWS ONLY) AS "t5"