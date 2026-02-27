SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "kind" AS "COMPANY_TYPE"
FROM "s1"
WHERE "production_year" = 2020
ORDER BY "name", "title"