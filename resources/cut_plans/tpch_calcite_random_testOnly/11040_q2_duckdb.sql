SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "p_name", "t1"."TOTAL_QUANTITY", "t1"."TOTAL_REVENUE", "t1"."AVG_SUPPLIER_BALANCE"
FROM (SELECT "s1"."p_name", SUM("s1"."l_quantity") AS "TOTAL_QUANTITY", SUM("s1"."l_extendedprice") AS "TOTAL_REVENUE", AVG("supplier"."s_acctbal") AS "AVG_SUPPLIER_BALANCE"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."customer" INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey" INNER JOIN "s1" ON "orders"."o_orderkey" = "s1"."l_orderkey") ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t1"