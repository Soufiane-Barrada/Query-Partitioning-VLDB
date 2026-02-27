SELECT COALESCE("t4"."AKA_NAME", "t4"."AKA_NAME") AS "AKA_NAME", "t4"."MOVIE_TITLE", "t4"."CAST_NOTE", "t4"."PERSON_INFO", "t4"."MOVIE_KEYWORD", "t4"."COMPANY_NAME", "t4"."ROLE_TYPE", "t4"."production_year"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t2"."title" AS "MOVIE_TITLE", "s1"."note" AS "CAST_NOTE", "person_info"."info" AS "PERSON_INFO", "keyword"."keyword" AS "MOVIE_KEYWORD", "company_name"."name" AS "COMPANY_NAME", "s1"."role" AS "ROLE_TYPE", "t2"."production_year"
FROM "IMDB"."company_name"
INNER JOIN ((SELECT SINGLE_VALUE("id") AS "$f0"
FROM "IMDB"."info_type"
WHERE "info" = 'Biography') AS "t1" INNER JOIN ("s1" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "s1"."person_id" = "aka_name"."person_id") ON "t1"."$f0" = "person_info"."info_type_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t2" INNER JOIN "IMDB"."movie_companies" ON "t2"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t2"."id") ON "s1"."movie_id" = "t2"."id") ON "company_name"."id" = "movie_companies"."company_id"
ORDER BY "t2"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t4"