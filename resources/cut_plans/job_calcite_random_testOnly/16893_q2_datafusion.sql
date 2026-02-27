SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."CAST_NOTE", "t2"."COMPANY_NAME", "t2"."MOVIE_KEYWORD", "t2"."production_year"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "cast_info"."note" AS "CAST_NOTE", "s1"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", "t0"."production_year"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t0"."id" = "movie_keyword"."movie_id") ON "cast_info"."movie_id" = "t0"."movie_id") ON "s1"."movie_id" = "t0"."id"
ORDER BY "t0"."production_year" DESC NULLS FIRST) AS "t2"