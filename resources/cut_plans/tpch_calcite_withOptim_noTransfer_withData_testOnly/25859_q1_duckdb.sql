SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "s_name", "t0"."r_name", ANY_VALUE(CONCAT("supplier"."s_name", ' from ', "t0"."r_name")) AS "SUPPLIER_INFO", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", SUM("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "TOTAL_REVENUE", AVG("part"."p_retailprice") AS "AVG_PART_PRICE", LISTAGG(DISTINCT "part"."p_name", ', ') AS "PART_NAMES"
FROM (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t"
INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN ((SELECT *
FROM "TPCH"."region"
WHERE "r_name" LIKE 'Europe%') AS "t0" INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t"."l_partkey" = "part"."p_partkey"
GROUP BY "supplier"."s_name", "t0"."r_name"