SELECT COALESCE("t"."p_brand", "t"."p_brand") AS "P_BRAND", "supplier"."s_suppkey", "partsupp"."ps_supplycost", "t1"."l_shipdate", TRIM(BOTH ' ' FROM "t"."p_name") AS "FD_COL_4", ', ' AS "FD_COL_5", "t1"."l_extendedprice" * (1 - "t1"."l_discount") AS "FD_COL_6"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 20) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."customer"
WHERE "c_mktsegment" = 'BUILDING') AS "t0" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'RAIL')) AS "t1" ON "orders"."o_orderkey" = "t1"."l_orderkey") ON "t0"."c_custkey" = "orders"."o_custkey") ON "t"."p_partkey" = "t1"."l_partkey"