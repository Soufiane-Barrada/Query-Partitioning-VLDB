SELECT COALESCE("t5"."NATION_NAME", "t5"."NATION_NAME") AS "NATION_NAME", "t5"."SUPPLIER_COUNT", "t5"."LARGE_PART_AVAIL_QTY", "t5"."AVG_RETAIL_PRICE", "t5"."PART_NAMES", "t5"."MAX_ORDER_TOTAL_PRICE"
FROM (SELECT "s1"."n_name", ANY_VALUE("s1"."n_name") AS "NATION_NAME", COUNT(DISTINCT "s1"."s_suppkey") AS "SUPPLIER_COUNT", SUM(CASE WHEN "s1"."p_size" > 10 THEN "s1"."ps_availqty" ELSE 0 END) AS "LARGE_PART_AVAIL_QTY", AVG("s1"."p_retailprice") AS "AVG_RETAIL_PRICE", LISTAGG(DISTINCT "s1"."p_name", ', ') AS "PART_NAMES", MAX("orders"."o_totalprice") AS "MAX_ORDER_TOTAL_PRICE"
FROM "s1"
LEFT JOIN "TPCH"."orders" ON "s1"."l_orderkey" = "orders"."o_orderkey"
GROUP BY "s1"."n_name"
HAVING COUNT(DISTINCT "s1"."s_suppkey") > 5
ORDER BY 2) AS "t5"