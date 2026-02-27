SELECT COALESCE("t5"."SUPPLIER_INFO", "t5"."SUPPLIER_INFO") AS "SUPPLIER_INFO", "t5"."TOTAL_ORDERS", "t5"."TOTAL_REVENUE", "t5"."AVG_PART_PRICE", "t5"."PART_NAMES"
FROM (SELECT "s1"."s_name", "s1"."r_name", ANY_VALUE(CONCAT("s1"."s_name", ' from ', "s1"."r_name")) AS "SUPPLIER_INFO", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", AVG("part"."p_retailprice") AS "AVG_PART_PRICE", LISTAGG(DISTINCT "part"."p_name", ', ') AS "PART_NAMES"
FROM (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1"
INNER JOIN "TPCH"."orders" ON "t1"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN ("s1" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."l_partkey" = "part"."p_partkey"
GROUP BY "s1"."s_name", "s1"."r_name"
HAVING SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) > 100000.0000
ORDER BY 5 DESC NULLS FIRST) AS "t5"