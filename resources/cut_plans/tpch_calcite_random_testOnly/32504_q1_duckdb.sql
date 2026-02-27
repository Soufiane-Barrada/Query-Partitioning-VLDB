SELECT COALESCE("c_custkey", "c_custkey") AS "C_CUSTKEY", "c_name" AS "C_NAME", "c_acctbal" AS "C_ACCTBAL", CASE WHEN "c_phone" <> '' THEN CAST(CASE WHEN "c_phone" = '' THEN NULL ELSE "c_phone" END AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'N/A' END AS "CONTACT_NUMBER"
FROM "TPCH"."customer"
WHERE "c_acctbal" > 1000.00