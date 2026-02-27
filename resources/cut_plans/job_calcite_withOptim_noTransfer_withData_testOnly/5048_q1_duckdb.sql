SELECT COALESCE("t0"."title", "t0"."title") AS "MOVIE_TITLE", "t0"."production_year" AS "PRODUCTION_YEAR", "aka_name"."name" AS "ACTOR_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", ', ' AS "FD_COL_4", "t"."name" AS "COMPANY_NAME"
FROM "IMDB"."cast_info"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "cast_info"."person_id" = "aka_name"."person_id"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."company_type" INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t" INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."company_id") ON "company_type"."id" = "movie_companies"."company_type_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" ON "complete_cast"."movie_id" = "t0"."id") ON "movie_companies"."movie_id" = "t0"."id") ON "movie_keyword"."movie_id" = "t0"."id") ON "cast_info"."id" = "complete_cast"."subject_id"