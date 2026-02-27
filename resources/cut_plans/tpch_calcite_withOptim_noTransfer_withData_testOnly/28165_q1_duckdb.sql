SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "P_NAME", COUNT(DISTINCT "t"."s_suppkey") AS "SUPPLIER_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("t0"."p_retailprice") AS "AVERAGE_PRICE", LISTAGG(DISTINCT CONCAT("t"."s_name", ' (', "t"."s_address", ')'), '; ') AS "SUPPLIER_DETAILS"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%steel%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_name"