SELECT COALESCE("t2"."MOVIE_TITLE", "t2"."MOVIE_TITLE") AS "MOVIE_TITLE", "t2"."ACTOR_NAME", "t2"."COMPANY_TYPE", "t2"."MOVIE_INFO", "t2"."production_year"
FROM (SELECT "t0"."title" AS "MOVIE_TITLE", "aka_name"."name" AS "ACTOR_NAME", "s1"."kind" AS "COMPANY_TYPE", "movie_info"."info" AS "MOVIE_INFO", "t0"."production_year"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."movie_info" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "IMDB"."aka_title" ON "t0"."id" = "aka_title"."movie_id") ON "movie_info"."movie_id" = "t0"."id") ON "cast_info"."movie_id" = "t0"."id" AND "aka_name"."person_id" = "aka_title"."id") ON "s1"."movie_id" = "t0"."id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t2"