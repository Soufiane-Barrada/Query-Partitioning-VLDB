SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "region"."r_name", "part"."p_partkey", "partsupp"."ps_availqty", "part"."p_retailprice", "supplier"."s_name", ', ' AS "FD_COL_6", "nation"."n_nationkey", "supplier"."s_suppkey"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "part"."p_retailprice" > 50.00 AND "supplier"."s_acctbal" > 1000.00