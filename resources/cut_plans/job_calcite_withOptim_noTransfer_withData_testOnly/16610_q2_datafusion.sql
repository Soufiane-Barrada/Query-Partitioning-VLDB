SELECT COALESCE("TITLE", "TITLE") AS "TITLE", "ACTOR_NAME", "COMPANY_TYPE", "production_year"
FROM (SELECT "title" AS "TITLE", "name" AS "ACTOR_NAME", "kind" AS "COMPANY_TYPE", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST, "name") AS "t2"