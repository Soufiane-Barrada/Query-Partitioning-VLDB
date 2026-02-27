SELECT COALESCE("t"."s_name", "t"."s_name") AS "s_name", "part"."p_name", ANY_VALUE("t"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("part"."p_name") AS "PART_NAME", COUNT(DISTINCT "t2"."o_orderkey") AS "TOTAL_ORDERS", SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) AS "TOTAL_REVENUE", LISTAGG(DISTINCT "t1"."c_mktsegment", ', ') AS "MARKET_SEGMENTS", MIN("t0"."l_shipdate") AS "FIRST_SHIP_DATE", MAX("t0"."l_shipdate") AS "LAST_SHIP_DATE"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_name" LIKE '%Supplier%') AS "t"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN ((SELECT *
FROM "TPCH"."customer"
WHERE "c_address" LIKE '%Street%') AS "t1" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t2" ON "t1"."c_custkey" = "t2"."o_custkey") ON "t0"."l_orderkey" = "t2"."o_orderkey") ON "part"."p_partkey" = "t0"."l_partkey"
GROUP BY "t"."s_name", "part"."p_name"