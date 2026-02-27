SELECT COALESCE("t5"."AKA_NAME", "t5"."AKA_NAME") AS "AKA_NAME", "t5"."MOVIE_TITLE", "t5"."CAST_NOTE", "t5"."COMPANY_NAME", "t5"."MOVIE_KEYWORD", "t5"."ROLE_TYPE", "t5"."PERSON_INFO", "t5"."production_year"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t3"."title" AS "MOVIE_TITLE", "s1"."note" AS "CAST_NOTE", "t0"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", "s1"."role" AS "ROLE_TYPE", "person_info"."info" AS "PERSON_INFO", "t3"."production_year"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t0"
INNER JOIN ((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Biography') AS "t2" INNER JOIN ("s1" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "s1"."person_id" = "aka_name"."person_id") ON "t2"."ID" = "person_info"."info_type_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t3" INNER JOIN "IMDB"."movie_companies" ON "t3"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t3"."id") ON "s1"."movie_id" = "t3"."id") ON "t0"."id" = "movie_companies"."company_id"
ORDER BY "t3"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t5"