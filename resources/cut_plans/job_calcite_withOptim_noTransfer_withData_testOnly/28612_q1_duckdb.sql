SELECT COALESCE("t1"."id", "t1"."id") AS "MOVIE_ID", "t1"."title" AS "MOVIE_TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", "aka_name"."name" AS "ACTOR_NAME", ', ' AS "FD_COL_4", "t"."keyword" AS "MOVIE_KEYWORD", "t0"."name" || ' (' || "company_type"."kind" || ')' AS "FD_COL_6"
FROM (SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%') AS "t"
INNER JOIN "IMDB"."movie_keyword" ON "t"."id" = "movie_keyword"."keyword_id"
INNER JOIN ("IMDB"."role_type" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "role_type"."id" = "cast_info"."role_id" INNER JOIN ("IMDB"."company_type" INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."company_id") ON "company_type"."id" = "movie_companies"."company_type_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t1" INNER JOIN "IMDB"."complete_cast" ON "t1"."id" = "complete_cast"."movie_id") ON "movie_companies"."movie_id" = "t1"."id") ON "cast_info"."person_id" = "complete_cast"."subject_id" AND "cast_info"."movie_id" = "complete_cast"."movie_id") ON "movie_keyword"."movie_id" = "t1"."id"