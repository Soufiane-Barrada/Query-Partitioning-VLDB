SELECT COALESCE("t3"."TITLE", "t3"."TITLE") AS "TITLE", "t3"."ACTOR_NAME", "t3"."COMPANY_TYPE", "t3"."MOVIE_INFO", "t3"."production_year"
FROM (SELECT "t1"."title" AS "TITLE", "aka_name"."name" AS "ACTOR_NAME", "s1"."kind" AS "COMPANY_TYPE", "movie_info"."info" AS "MOVIE_INFO", "t1"."production_year"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."movie_info" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "movie_info"."movie_id" = "t1"."id") ON "cast_info"."movie_id" = "t1"."id") ON "s1"."id" = "movie_companies"."company_type_id"
ORDER BY "t1"."production_year" DESC NULLS FIRST) AS "t3"