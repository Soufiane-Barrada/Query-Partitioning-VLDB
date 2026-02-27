SELECT COALESCE("id", "id") AS "id", "kind"
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%production%'