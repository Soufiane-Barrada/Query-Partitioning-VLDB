SELECT COALESCE("t2"."ACTOR_NAME", "t2"."ACTOR_NAME") AS "ACTOR_NAME", "t2"."MOVIE_TITLE", "t2"."PRODUCTION_YEAR", "t2"."COMPANY_TYPE", "t2"."MOVIE_KEYWORD"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "t0"."title" AS "MOVIE_TITLE", "t0"."production_year" AS "PRODUCTION_YEAR", "company_type"."kind" AS "COMPANY_TYPE", "s1"."keyword" AS "MOVIE_KEYWORD"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN ("IMDB"."company_type" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "company_type"."id" = "movie_companies"."company_type_id" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "cast_info"."movie_id" = "t0"."movie_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t2"