SELECT COALESCE("t8"."PRODUCTION_YEAR", "t8"."PRODUCTION_YEAR") AS "PRODUCTION_YEAR", "t8"."TITLE_COUNT", "t8"."AVG_TITLE_LENGTH", "t8"."COMPANY_TYPE", "t8"."COMPANY_COUNT"
FROM (SELECT "t4"."PRODUCTION_YEAR", "t4"."TITLE_COUNT", "t4"."AVG_TITLE_LENGTH", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE", COUNT(DISTINCT "t4"."company_id") AS "COMPANY_COUNT"
FROM "IMDB"."company_type"
INNER JOIN (SELECT "s1"."PRODUCTION_YEAR", "s1"."TITLE_COUNT", "s1"."AVG_TITLE_LENGTH", "s1"."MAX_RANK", "movie_companies"."id", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note"
FROM "s1",
"IMDB"."movie_companies") AS "t4" ON "company_type"."id" = "t4"."company_type_id"
GROUP BY "company_type"."kind", "t4"."PRODUCTION_YEAR", "t4"."TITLE_COUNT", "t4"."AVG_TITLE_LENGTH"
ORDER BY "t4"."PRODUCTION_YEAR", "t4"."AVG_TITLE_LENGTH" DESC NULLS FIRST) AS "t8"