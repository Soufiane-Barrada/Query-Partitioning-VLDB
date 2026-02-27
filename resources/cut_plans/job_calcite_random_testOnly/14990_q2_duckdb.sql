SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."CAST_ORDER", "t2"."PERSON_GENDER", "t2"."COMPANY_TYPE", "t2"."MOVIE_KEYWORD", "t2"."production_year"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "cast_info"."nr_order" AS "CAST_ORDER", "name"."gender" AS "PERSON_GENDER", "company_type"."kind" AS "COMPANY_TYPE", "s1"."keyword" AS "MOVIE_KEYWORD", "t0"."production_year"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."cast_info" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."name" ON "aka_name"."person_id" = "name"."imdb_id") ON "cast_info"."person_id" = "aka_name"."person_id" INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "s1"."movie_id" = "t0"."id") ON "cast_info"."movie_id" = "t0"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "cast_info"."nr_order") AS "t2"