SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "supplier"."s_suppkey", CASE WHEN "part"."p_size" > 10 THEN "partsupp"."ps_availqty" ELSE 0 END AS "FD_COL_2", "part"."p_retailprice", "part"."p_name", ', ' AS "FD_COL_5", "orders"."o_totalprice"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
LEFT JOIN "TPCH"."lineitem" ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
LEFT JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "nation"."n_name" LIKE 'N%'