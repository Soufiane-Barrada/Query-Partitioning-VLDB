SELECT COALESCE("t4"."MOVIE_ID", "t4"."MOVIE_ID") AS "MOVIE_ID", "t4"."TITLE", "t4"."PRODUCTION_YEAR", "t4"."AKA_NAMES", "t4"."CAST_COUNT", COUNT(DISTINCT "company_name"."id") AS "COMPANY_COUNT", LISTAGG(DISTINCT "company_name"."name", ', ') AS "COMPANIES"
FROM "IMDB"."company_name"
RIGHT JOIN (SELECT "t3"."MOVIE_ID", "t3"."TITLE", "t3"."PRODUCTION_YEAR", "t3"."AKA_NAMES", "t3"."CAST_COUNT", "t3"."KEYWORDS", "movie_companies"."id", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note"
FROM "IMDB"."movie_companies"
RIGHT JOIN (SELECT ANY_VALUE("t0"."id") AS "MOVIE_ID", "t0"."title" AS "TITLE", "t0"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "s1"."name", ', ') AS "AKA_NAMES", COUNT(DISTINCT "s1"."person_id0") AS "CAST_COUNT", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS"
FROM "s1"
INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."complete_cast" ON "t0"."movie_id" = "complete_cast"."movie_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t0"."movie_id" = "movie_keyword"."movie_id") ON "s1"."person_id0" = "complete_cast"."subject_id"
GROUP BY "t0"."id", "t0"."title", "t0"."production_year") AS "t3" ON "movie_companies"."movie_id" = "t3"."MOVIE_ID") AS "t4" ON "company_name"."id" = "t4"."company_id"
GROUP BY "t4"."MOVIE_ID", "t4"."TITLE", "t4"."PRODUCTION_YEAR", "t4"."AKA_NAMES", "t4"."CAST_COUNT"
ORDER BY "t4"."PRODUCTION_YEAR" DESC NULLS FIRST, "t4"."CAST_COUNT" DESC NULLS FIRST