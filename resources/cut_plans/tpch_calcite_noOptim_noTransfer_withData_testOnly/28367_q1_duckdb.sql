SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "supplier"."s_suppkey", "partsupp"."ps_supplycost", "lineitem"."l_shipdate", TRIM(BOTH ' ' FROM "part"."p_name") AS "FD_COL_4", ', ' AS "FD_COL_5", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_6"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
WHERE "part"."p_size" > 20 AND ("lineitem"."l_shipmode" = 'AIR' OR "lineitem"."l_shipmode" = 'RAIL') AND "customer"."c_mktsegment" = 'BUILDING'