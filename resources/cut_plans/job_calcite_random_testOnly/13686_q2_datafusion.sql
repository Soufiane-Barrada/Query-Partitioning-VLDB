SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."CAST_NOTE", "t2"."ACTOR_NAME", "t2"."COMPANY_TYPE", "t2"."MOVIE_INFO", "t2"."MOVIE_KEYWORD", "t2"."production_year"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "s1"."note" AS "CAST_NOTE", "s1"."name" AS "ACTOR_NAME", "company_type"."kind" AS "COMPANY_TYPE", "movie_info"."info" AS "MOVIE_INFO", "keyword"."keyword" AS "MOVIE_KEYWORD", "t0"."production_year"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."movie_info" INNER JOIN ("IMDB"."aka_name" INNER JOIN "s1" ON "aka_name"."person_id" = "s1"."person_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "s1"."movie_id" = "t0"."id") ON "movie_info"."movie_id" = "t0"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST) AS "t2"