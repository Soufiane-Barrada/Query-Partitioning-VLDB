SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "P_NAME", CASE WHEN "lineitem"."l_returnflag" = 'R' THEN "lineitem"."l_quantity" ELSE 0.00 END AS "FD_COL_1", "t"."o_orderkey", "lineitem"."l_extendedprice", CONCAT("supplier"."s_name", ' (', "supplier"."s_phone", ')') AS "FD_COL_4", '; ' AS "FD_COL_5"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."supplier" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%Steel%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t0"."p_partkey"