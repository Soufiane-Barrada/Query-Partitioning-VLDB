SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "p_name", COUNT(DISTINCT "t1"."p_partkey") AS "DISTINCT_PART_COUNT", AVG("t0"."s_acctbal") AS "AVERAGE_SUPPLIER_BALANCE", LISTAGG(DISTINCT CONCAT("t"."n_name", ': ', "t0"."s_name"), ', ') AS "SUPPLIER_NATIONS", COUNT(*) AS "FD_COL_4"
FROM (SELECT *
FROM "TPCH"."nation"
WHERE "n_name" IN ('Germany', 'Japan', 'USA')) AS "t"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" LIKE '%reliable%') AS "t0" ON "t"."n_nationkey" = "t0"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 50.00) AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t1"."p_name"
HAVING COUNT(*) > 2