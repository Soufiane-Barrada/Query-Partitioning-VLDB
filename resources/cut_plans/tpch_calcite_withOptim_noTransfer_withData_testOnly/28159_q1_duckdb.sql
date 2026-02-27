SELECT COALESCE("t"."s_name", "t"."s_name") AS "s_name", "region"."r_name", ANY_VALUE("t"."s_name") AS "SUPPLIER_NAME", COUNT(DISTINCT "t0"."p_partkey") AS "TOTAL_PARTS_SUPPLIED", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", LISTAGG(DISTINCT "t0"."p_name", ', ') AS "SUPPLIED_PART_NAMES", ANY_VALUE("region"."r_name") AS "REGION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 500.00) AS "t" ON "nation"."n_nationkey" = "t"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t"."s_name", "region"."r_name"
HAVING COUNT(DISTINCT "t0"."p_partkey") > 5