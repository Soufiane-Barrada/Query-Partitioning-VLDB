SELECT COALESCE("t4"."MOVIE_TITLE", "t4"."MOVIE_TITLE") AS "MOVIE_TITLE", "t4"."PRODUCTION_YEAR", "t4"."ACTOR_NAMES", "t4"."MOVIE_KEYWORD", "t4"."COMPANY_TYPE", "t4"."ACTOR_COUNT"
FROM (SELECT "t0"."id", "t0"."title", "t0"."production_year" AS "PRODUCTION_YEAR", "s1"."keyword", "company_type"."kind", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", LISTAGG(DISTINCT "aka_name"."name", ', ') AS "ACTOR_NAMES", ANY_VALUE("s1"."keyword") AS "MOVIE_KEYWORD", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE", COUNT(DISTINCT "cast_info"."person_id") AS "ACTOR_COUNT"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."company_type" INNER JOIN "IMDB"."movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."complete_cast" ON "t0"."id" = "complete_cast"."movie_id") ON "movie_companies"."movie_id" = "t0"."id") ON "cast_info"."id" = "complete_cast"."subject_id") ON "s1"."movie_id" = "t0"."id"
GROUP BY "t0"."id", "t0"."title", "t0"."production_year", "s1"."keyword", "company_type"."kind"
HAVING COUNT(DISTINCT "cast_info"."person_id") > 2
ORDER BY "t0"."production_year" DESC NULLS FIRST, 6) AS "t4"