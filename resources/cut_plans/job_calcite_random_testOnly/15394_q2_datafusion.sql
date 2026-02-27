SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "COMPANY_TYPE", "production_year"
FROM (SELECT "name" AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "kind" AS "COMPANY_TYPE", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST) AS "t2"