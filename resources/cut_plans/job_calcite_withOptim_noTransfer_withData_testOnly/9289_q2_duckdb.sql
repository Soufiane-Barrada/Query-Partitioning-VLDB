SELECT COALESCE("t4"."AKA_NAME", "t4"."AKA_NAME") AS "AKA_NAME", "t4"."MOVIE_TITLE", "t4"."PERSON_INFO", "t4"."COMPANY_TYPE", "t4"."MOVIE_KEYWORD", "t4"."ROLE_NAME", "t4"."YEAR"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t2"."title" AS "MOVIE_TITLE", "person_info"."info" AS "PERSON_INFO", "company_type"."kind" AS "COMPANY_TYPE", "t0"."keyword" AS "MOVIE_KEYWORD", "s1"."role" AS "ROLE_NAME", "t2"."production_year" AS "YEAR"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "s1"."person_id" = "aka_name"."person_id"
INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%Drama%') AS "t0" INNER JOIN "IMDB"."movie_keyword" ON "t0"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."company_type" INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."company_id") ON "company_type"."id" = "movie_companies"."company_type_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t2" ON "complete_cast"."movie_id" = "t2"."id") ON "movie_companies"."movie_id" = "t2"."id") ON "movie_keyword"."movie_id" = "t2"."id") ON "s1"."movie_id" = "t2"."id"
ORDER BY "t2"."production_year" DESC NULLS FIRST, "aka_name"."name", "t2"."title") AS "t4"