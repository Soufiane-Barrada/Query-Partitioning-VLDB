SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."PERSON_NAME", "t2"."CAST_NOTE", "t2"."MOVIE_KEYWORD", "t2"."COMPANY_TYPE", "t2"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "s1"."name0" AS "PERSON_NAME", "cast_info"."note" AS "CAST_NOTE", "keyword"."keyword" AS "MOVIE_KEYWORD", "company_type"."kind" AS "COMPANY_TYPE", "t0"."production_year"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."cast_info" INNER JOIN "s1" ON "cast_info"."person_id" = "s1"."person_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "cast_info"."movie_id" = "t0"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "s1"."name") AS "t2"