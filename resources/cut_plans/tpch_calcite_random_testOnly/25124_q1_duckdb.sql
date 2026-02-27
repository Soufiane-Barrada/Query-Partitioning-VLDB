SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "region"."r_name", ANY_VALUE(CONCAT("nation"."n_name", ' - ', "region"."r_name")) AS "NATION_REGION", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", SUM(CASE WHEN LENGTH("part"."p_name") > 20 THEN "part"."p_retailprice" ELSE 0.00 END) AS "TOTAL_RETAIL_PRICE_LARGE_NAME", AVG("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "FD_COL_5", LISTAGG(DISTINCT "part"."p_comment", ', ') AS "PART_COMMENTS"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t" INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey") ON "partsupp"."ps_partkey" = "t"."l_partkey") ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "nation"."n_name", "region"."r_name"