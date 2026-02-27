SELECT COALESCE("title", "title") AS "MOVIE_TITLE", "name0" AS "ACTOR_NAME", "role_id" AS "ROLE_ID"
FROM "s1"
WHERE "production_year" = 2023
ORDER BY "title"