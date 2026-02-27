SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."title", "t"."production_year" AS "PRODUCTION_YEAR", "keyword"."keyword", "company_type"."kind", ANY_VALUE("t"."title") AS "MOVIE_TITLE", LISTAGG(DISTINCT "aka_name"."name", ', ') AS "ACTOR_NAMES", ANY_VALUE("keyword"."keyword") AS "MOVIE_KEYWORD", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE", COUNT(DISTINCT "cast_info"."person_id") AS "ACTOR_COUNT"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."company_type" INNER JOIN "IMDB"."movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t" INNER JOIN "IMDB"."complete_cast" ON "t"."id" = "complete_cast"."movie_id") ON "movie_companies"."movie_id" = "t"."id") ON "cast_info"."id" = "complete_cast"."subject_id") ON "movie_keyword"."movie_id" = "t"."id"
GROUP BY "t"."id", "t"."title", "t"."production_year", "keyword"."keyword", "company_type"."kind"