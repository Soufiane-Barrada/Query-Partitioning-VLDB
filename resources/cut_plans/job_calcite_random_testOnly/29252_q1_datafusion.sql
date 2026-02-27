SELECT COALESCE("t1"."title", "t1"."title") AS "MOVIE_TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", "aka_name"."name" AS "ACTOR_NAME", ', ' AS "FD_COL_3", "company_type"."kind" AS "COMPANY_TYPE", "movie_info"."info" AS "MOVIE_INFO", '; ' AS "FD_COL_6", "keyword"."keyword" AS "MOVIE_KEYWORD"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ((SELECT SINGLE_VALUE("id") AS "$f0"
FROM "IMDB"."info_type"
WHERE "info" = 'Synopsis') AS "t0" INNER JOIN "IMDB"."movie_info" ON "t0"."$f0" = "movie_info"."info_type_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "movie_info"."movie_id" = "t1"."id") ON "cast_info"."movie_id" = "t1"."id") ON "movie_keyword"."movie_id" = "t1"."id") ON "company_type"."id" = "movie_companies"."company_type_id"