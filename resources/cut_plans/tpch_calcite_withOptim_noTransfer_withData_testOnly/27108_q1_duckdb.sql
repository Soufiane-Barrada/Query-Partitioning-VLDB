SELECT COALESCE("t0"."p_brand", "t0"."p_brand") AS "P_BRAND", "region"."r_name", "t0"."p_partkey", "partsupp"."ps_availqty", "t0"."p_retailprice", "t"."s_name", ', ' AS "FD_COL_6", "nation"."n_nationkey", "t"."s_suppkey"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t" ON "nation"."n_nationkey" = "t"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 50.00) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"