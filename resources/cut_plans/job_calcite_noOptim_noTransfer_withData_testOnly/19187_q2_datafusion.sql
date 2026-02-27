SELECT COALESCE("title", "title") AS "TITLE", "name0" AS "ACTOR_NAME", "info" AS "ACTOR_INFO"
FROM "s1"
WHERE "production_year" = 2020
ORDER BY "title", "name0"