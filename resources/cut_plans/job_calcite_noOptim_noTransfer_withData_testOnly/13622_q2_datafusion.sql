SELECT COALESCE("title", "title") AS "TITLE", "name" AS "ACTOR_NAME", "kind" AS "ROLE", "production_year"
FROM "s1"
WHERE "production_year" >= 2000
ORDER BY "production_year" DESC NULLS FIRST