SELECT COALESCE("t2"."ALIAS_NAME", "t2"."ALIAS_NAME") AS "ALIAS_NAME", "t2"."MOVIE_TITLE", "t2"."ACTOR_NAME", "t2"."ROLE_ID", "t2"."MOVIE_INFO", "t2"."production_year"
FROM (SELECT "aka_name"."name" AS "ALIAS_NAME", "t0"."title" AS "MOVIE_TITLE", "s1"."name" AS "ACTOR_NAME", "s1"."role_id" AS "ROLE_ID", "movie_info"."info" AS "MOVIE_INFO", "t0"."production_year"
FROM "IMDB"."aka_name"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_info" ON "t0"."id" = "movie_info"."movie_id" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "aka_name"."person_id" = "s1"."person_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t2"