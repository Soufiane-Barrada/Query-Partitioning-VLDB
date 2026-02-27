SELECT COALESCE("t14"."MOVIE_ID", "t14"."MOVIE_ID") AS "MOVIE_ID", "t14"."TITLE", "t14"."PRODUCTION_YEAR", "t14"."ACTORS", "t14"."KEYWORDS", "t14"."COMPANY_TYPES", "t14"."AVG_YEAR", "t14"."TOTAL_ACTORS"
FROM (SELECT "t12"."MOVIE_ID", "t12"."TITLE", "t12"."PRODUCTION_YEAR", "t12"."ACTORS", "t12"."KEYWORDS", "t12"."COMPANY_TYPES", "s1"."AVG_YEAR", "t6"."TOTAL_ACTORS"
FROM "s1",
(SELECT COUNT(DISTINCT "person_id") AS "TOTAL_ACTORS"
FROM "IMDB"."aka_name") AS "t6",
(SELECT ANY_VALUE("t9"."id") AS "MOVIE_ID", "t9"."title" AS "TITLE", "t9"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "aka_name1"."name", ', ') AS "ACTORS", LISTAGG(DISTINCT "keyword0"."keyword", ', ') AS "KEYWORDS", LISTAGG(DISTINCT "company_type0"."kind", ', ') AS "COMPANY_TYPES"
FROM "IMDB"."keyword" AS "keyword0"
INNER JOIN "IMDB"."movie_keyword" AS "movie_keyword0" ON "keyword0"."id" = "movie_keyword0"."keyword_id"
INNER JOIN ("IMDB"."company_type" AS "company_type0" INNER JOIN "IMDB"."movie_companies" AS "movie_companies0" ON "company_type0"."id" = "movie_companies0"."company_type_id" INNER JOIN ("IMDB"."aka_name" AS "aka_name1" INNER JOIN "IMDB"."cast_info" AS "cast_info0" ON "aka_name1"."person_id" = "cast_info0"."person_id" INNER JOIN ((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Synopsis') AS "t8" INNER JOIN "IMDB"."movie_info" AS "movie_info0" ON "t8"."ID" = "movie_info0"."info_type_id" INNER JOIN ("IMDB"."complete_cast" AS "complete_cast0" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t9" ON "complete_cast0"."movie_id" = "t9"."id") ON "movie_info0"."movie_id" = "t9"."id") ON "cast_info0"."person_id" = "complete_cast0"."subject_id") ON "movie_companies0"."movie_id" = "t9"."id") ON "movie_keyword0"."movie_id" = "t9"."id"
GROUP BY "t9"."id", "t9"."title", "t9"."production_year") AS "t12"
ORDER BY "t12"."PRODUCTION_YEAR" DESC NULLS FIRST) AS "t14"