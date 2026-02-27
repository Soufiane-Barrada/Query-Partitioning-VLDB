SELECT COALESCE("t"."p_partkey", "t"."p_partkey") AS "p_partkey", "t"."p_name", "t"."p_mfgr", "t"."p_brand", "region"."r_name", "nation"."n_name", CONCAT('Part Name: ', "t"."p_name", ' - Manufacturer: ', "t"."p_mfgr", ' - Brand: ', "t"."p_brand") AS "FD_COL_6", CASE WHEN "lineitem"."l_returnflag" = 'Y' THEN "lineitem"."l_quantity" ELSE 0.00 END AS "FD_COL_7", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_8", "t0"."o_orderkey"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_comment" LIKE '%fragile%') AS "t" INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" <= DATE '1996-12-31') AS "t0" INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey") ON "t"."p_partkey" = "lineitem"."l_partkey") ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"