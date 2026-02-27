SELECT COALESCE("t"."p_name", "t"."p_name") AS "P_NAME", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", SUM(CASE WHEN "lineitem"."l_discount" > 0.10 THEN "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") ELSE CAST("lineitem"."l_extendedprice" AS DECIMAL(19, 4)) END) AS "TOTAL_DISCOUNTED_SALES", AVG("lineitem"."l_quantity") AS "AVG_QUANTITY_PER_LINE", MAX("lineitem"."l_shipdate") AS "LATEST_SHIPDATE", MIN("lineitem"."l_shipdate") AS "EARLIEST_SHIPDATE", LISTAGG(DISTINCT CONCAT('Supplier ', "supplier"."s_name", ' in ', "nation"."n_name"), ', ') AS "SUPPLIER_DETAILS"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "t"."p_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t"."p_name"