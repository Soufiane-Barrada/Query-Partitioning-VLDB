SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."CAST_NOTE", "t2"."COMPANY_NAME", "t2"."MOVIE_KEYWORD", "t2"."ROLE_NAME", "t2"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "s1"."note" AS "CAST_NOTE", "company_name"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", "role_type"."role" AS "ROLE_NAME", "t0"."production_year"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
INNER JOIN ("IMDB"."role_type" INNER JOIN "s1" ON "role_type"."id" = "s1"."role_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t0"."id" = "movie_keyword"."movie_id") ON "s1"."movie_id" = "t0"."movie_id") ON "movie_companies"."movie_id" = "t0"."id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "s1"."name") AS "t2"