SELECT COALESCE("t0"."p_brand", "t0"."p_brand") AS "P_BRAND", "region"."r_name", COUNT(DISTINCT "t0"."p_partkey") AS "UNIQUE_PARTS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAIL_QTY", AVG("t0"."p_retailprice") AS "AVG_RETAIL_PRICE", LISTAGG(DISTINCT "t"."s_name", ', ') AS "SUPPLIERS", COUNT(DISTINCT "nation"."n_nationkey") AS "FD_COL_6", COUNT(DISTINCT "t"."s_suppkey") AS "FD_COL_7"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t" ON "nation"."n_nationkey" = "t"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 50.00) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_brand", "region"."r_name"
HAVING COUNT(DISTINCT "t"."s_suppkey") > 2