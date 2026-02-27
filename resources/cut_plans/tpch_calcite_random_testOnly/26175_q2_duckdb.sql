SELECT COALESCE("t11"."S_NAME", "t11"."S_NAME") AS "S_NAME", "t11"."P_NAME", "t11"."TOTAL_PARTS", "t11"."TOTAL_COST", "t11"."REGION_NAME", "t11"."CUSTOMER_NAME", "t11"."TOTAL_ORDERS", "t11"."AVERAGE_ORDER_VALUE"
FROM (SELECT "s1"."S_NAME", "s1"."P_NAME", "s1"."TOTAL_PARTS", "s1"."TOTAL_COST", "s1"."R_NAME" AS "REGION_NAME", "t8"."C_NAME" AS "CUSTOMER_NAME", "t8"."TOTAL_ORDERS", "t8"."AVERAGE_ORDER_VALUE"
FROM (SELECT "customer"."c_custkey" AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", COUNT(*) AS "TOTAL_ORDERS", AVG("orders"."o_totalprice") AS "AVERAGE_ORDER_VALUE", COUNT(*) > 5 AS "$f4"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name"
HAVING COUNT(*) > 5) AS "t8",
"s1"
WHERE "s1"."TOTAL_COST" > 5000.00
ORDER BY "s1"."TOTAL_PARTS" DESC NULLS FIRST, "t8"."AVERAGE_ORDER_VALUE" DESC NULLS FIRST) AS "t11"