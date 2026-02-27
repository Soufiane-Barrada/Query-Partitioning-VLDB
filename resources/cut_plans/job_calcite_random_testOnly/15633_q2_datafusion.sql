SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."ACTOR_NAME", "t2"."ROLE"
FROM (SELECT "t0"."title" AS "TITLE", "s1"."name" AS "ACTOR_NAME", "comp_cast_type"."kind" AS "ROLE"
FROM "IMDB"."comp_cast_type"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2020) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id" INNER JOIN "s1" ON "movie_companies"."movie_id" = "s1"."movie_id") ON "comp_cast_type"."id" = "s1"."person_role_id"
ORDER BY "t0"."title") AS "t2"