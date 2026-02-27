SELECT COALESCE("t"."l_shipmode", "t"."l_shipmode") AS "L_SHIPMODE", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT", SUM("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "REVENUE"
FROM "TPCH"."orders"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" < DATE '1996-12-31') AS "t" ON "orders"."o_orderkey" = "t"."l_orderkey"
GROUP BY "t"."l_shipmode"