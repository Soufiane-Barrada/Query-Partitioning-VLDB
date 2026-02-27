SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", SUM(CASE WHEN "lineitem"."l_discount" > 0.10 THEN "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") ELSE CAST("lineitem"."l_extendedprice" AS DECIMAL(19, 4)) END) AS "TOTAL_DISCOUNTED_SALES", AVG("lineitem"."l_quantity") AS "AVG_QUANTITY_PER_LINE", MAX("lineitem"."l_shipdate") AS "LATEST_SHIPDATE", MIN("lineitem"."l_shipdate") AS "EARLIEST_SHIPDATE", LISTAGG(DISTINCT CONCAT('Supplier ', "supplier"."s_name", ' in ', "nation"."n_name"), ', ') AS "SUPPLIER_DETAILS"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
WHERE "part"."p_retailprice" > 100.00
GROUP BY "part"."p_name"
HAVING COUNT(DISTINCT "supplier"."s_suppkey") > 5