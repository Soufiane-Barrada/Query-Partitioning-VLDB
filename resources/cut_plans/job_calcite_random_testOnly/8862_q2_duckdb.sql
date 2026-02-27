SELECT COALESCE("t5"."AKA_NAME", "t5"."AKA_NAME") AS "AKA_NAME", "t5"."MOVIE_TITLE", "t5"."CAST_NOTE", "t5"."COMPANY_NAME", "t5"."MOVIE_KEYWORD", "t5"."ROLE_TYPE", "t5"."PERSON_INFO", "t5"."production_year"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "s1"."title" AS "MOVIE_TITLE", "cast_info"."note" AS "CAST_NOTE", "t1"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", "role_type"."role" AS "ROLE_TYPE", "person_info"."info" AS "PERSON_INFO", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t1"
INNER JOIN ((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Biography') AS "t3" INNER JOIN ("IMDB"."role_type" INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."role_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "cast_info"."person_id" = "aka_name"."person_id") ON "t3"."ID" = "person_info"."info_type_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("s1" INNER JOIN "IMDB"."movie_companies" ON "s1"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "s1"."id") ON "cast_info"."movie_id" = "s1"."id") ON "t1"."id" = "movie_companies"."company_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t5"