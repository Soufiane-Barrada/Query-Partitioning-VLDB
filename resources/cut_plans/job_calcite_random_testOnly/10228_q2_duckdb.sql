SELECT COALESCE("t2"."ACTOR_NAME", "t2"."ACTOR_NAME") AS "ACTOR_NAME", "t2"."MOVIE_TITLE", "t2"."PRODUCTION_YEAR", "t2"."CAST_TYPE", "t2"."MOVIE_KEYWORD"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "t0"."title" AS "MOVIE_TITLE", "t0"."production_year" AS "PRODUCTION_YEAR", "comp_cast_type"."kind" AS "CAST_TYPE", "s1"."keyword" AS "MOVIE_KEYWORD"
FROM "IMDB"."comp_cast_type"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "comp_cast_type"."id" = "cast_info"."person_role_id"
INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "cast_info"."movie_id" = "t0"."id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t2"