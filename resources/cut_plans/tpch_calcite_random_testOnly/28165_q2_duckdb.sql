SELECT COALESCE("t5"."P_NAME", "t5"."P_NAME") AS "P_NAME", "t5"."SUPPLIER_COUNT", "t5"."TOTAL_AVAILABLE_QUANTITY", "t5"."AVERAGE_PRICE", "t5"."SUPPLIER_DETAILS"
FROM (SELECT "s1"."p_name" AS "P_NAME", COUNT(DISTINCT "t1"."s_suppkey") AS "SUPPLIER_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("s1"."p_retailprice") AS "AVERAGE_PRICE", LISTAGG(DISTINCT CONCAT("t1"."s_name", ' (', "t1"."s_address", ')'), '; ') AS "SUPPLIER_DETAILS"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t1"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."p_name"
HAVING COUNT(DISTINCT "t1"."s_suppkey") > 2
ORDER BY 4 DESC NULLS FIRST) AS "t5"