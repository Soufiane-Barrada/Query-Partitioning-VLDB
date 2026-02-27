SELECT COALESCE("t2"."MOVIE_TITLE", "t2"."MOVIE_TITLE") AS "MOVIE_TITLE", "t2"."PERSON_NAME", "t2"."AKA_NAME", "t2"."COMPANY_TYPE", "t2"."MOVIE_KEYWORD", "t2"."PERSON_ROLE"
FROM (SELECT "t0"."title" AS "MOVIE_TITLE", "name"."name" AS "PERSON_NAME", "aka_name"."name" AS "AKA_NAME", "company_type"."kind" AS "COMPANY_TYPE", "keyword"."keyword" AS "MOVIE_KEYWORD", "s1"."role" AS "PERSON_ROLE"
FROM "IMDB"."company_type"
INNER JOIN ("s1" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."name" ON "aka_name"."person_id" = "name"."imdb_id") ON "s1"."person_id" = "aka_name"."person_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "s1"."movie_id" = "t0"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
ORDER BY "t0"."title", "name"."name") AS "t2"