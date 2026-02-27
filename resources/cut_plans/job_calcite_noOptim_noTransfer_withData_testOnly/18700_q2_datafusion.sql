SELECT COALESCE("title", "title") AS "TITLE", "name0" AS "ACTOR_NAME", "info" AS "ACTOR_INFO", "production_year"
FROM "s1"
WHERE "production_year" >= 2000
ORDER BY "production_year" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY