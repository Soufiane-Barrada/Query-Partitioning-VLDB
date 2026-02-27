SELECT COALESCE("title", "title") AS "TITLE", "name" AS "ACTOR_NAME", "kind" AS "ROLE"
FROM "s1"
WHERE "production_year" = 2020
ORDER BY "title"