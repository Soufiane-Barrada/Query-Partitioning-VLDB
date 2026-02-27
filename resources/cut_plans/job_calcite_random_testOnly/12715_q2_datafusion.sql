SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."CAST_ORDER", "t2"."PERSON_NAME", "t2"."COMPANY_NAME", "t2"."KEYWORD", "t2"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "s1"."nr_order" AS "CAST_ORDER", "s1"."name0" AS "PERSON_NAME", "company_name"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "KEYWORD", "t0"."production_year"
FROM "IMDB"."company_name"
INNER JOIN ("s1" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "s1"."movie_id" = "t0"."id") ON "company_name"."imdb_id" = "movie_companies"."company_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "s1"."nr_order") AS "t2"