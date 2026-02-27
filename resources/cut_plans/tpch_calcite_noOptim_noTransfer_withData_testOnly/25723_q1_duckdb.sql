SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "p_partkey", "part"."p_name", "part"."p_mfgr", "part"."p_brand", "region"."r_name", "nation"."n_name", CONCAT('Part Name: ', "part"."p_name", ' - Manufacturer: ', "part"."p_mfgr", ' - Brand: ', "part"."p_brand") AS "FD_COL_6", CASE WHEN "lineitem"."l_returnflag" = 'Y' THEN "lineitem"."l_quantity" ELSE 0.00 END AS "FD_COL_7", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_8", "orders"."o_orderkey"
FROM "TPCH"."part"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."supplier" ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "part"."p_comment" LIKE '%fragile%' AND "orders"."o_orderdate" >= '1996-01-01' AND "orders"."o_orderdate" <= '1996-12-31'