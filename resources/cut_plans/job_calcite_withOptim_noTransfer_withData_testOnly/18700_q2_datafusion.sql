SELECT COALESCE("TITLE", "TITLE") AS "TITLE", "ACTOR_NAME", "ACTOR_INFO", "production_year"
FROM (SELECT "title" AS "TITLE", "name0" AS "ACTOR_NAME", "info" AS "ACTOR_INFO", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"