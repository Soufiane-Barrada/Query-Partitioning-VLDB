SELECT COALESCE("t11"."TITLE", "t11"."TITLE") AS "TITLE", "t11"."PRODUCTION_YEAR", "t11"."TOTAL_CAST", "t11"."CAST_NAMES", "t11"."TOTAL_COMPANIES", "t11"."COMPANIES"
FROM (SELECT "s1"."TITLE", "s1"."PRODUCTION_YEAR", "s1"."TOTAL_CAST", "s1"."CAST_NAMES", "t8"."TOTAL_COMPANIES", "t8"."COMPANIES"
FROM (SELECT "movie_companies"."movie_id" AS "MOVIE_ID", LISTAGG(DISTINCT "company_name"."name", ', ') AS "COMPANIES", COUNT(DISTINCT "movie_companies"."company_id") AS "TOTAL_COMPANIES"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
GROUP BY "movie_companies"."movie_id") AS "t8"
RIGHT JOIN "s1" ON "t8"."MOVIE_ID" = "s1"."MOVIE_ID"
WHERE "t8"."TOTAL_COMPANIES" IS NULL OR "t8"."TOTAL_COMPANIES" > 2
ORDER BY "s1"."PRODUCTION_YEAR" DESC NULLS FIRST, "s1"."TITLE") AS "t11"