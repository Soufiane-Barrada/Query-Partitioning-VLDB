SELECT COALESCE("t6"."MOVIE_TITLE", "t6"."MOVIE_TITLE") AS "MOVIE_TITLE", "t6"."PRODUCTION_YEAR", "t6"."NUM_ACTORS", "t6"."NUM_KEYWORDS", "t6"."COMPANIES"
FROM (SELECT "s1"."id", "s1"."title", "s1"."production_year" AS "PRODUCTION_YEAR", ANY_VALUE("s1"."title") AS "MOVIE_TITLE", COUNT(DISTINCT "s1"."person_id") AS "NUM_ACTORS", COUNT(DISTINCT "s1"."keyword") AS "NUM_KEYWORDS", LISTAGG(DISTINCT "company_name"."name", ', ') AS "COMPANIES"
FROM "s1"
LEFT JOIN "IMDB"."company_name" ON "s1"."company_id" = "company_name"."id"
GROUP BY "s1"."id", "s1"."title", "s1"."production_year"
HAVING COUNT(DISTINCT "s1"."person_id") > 5
ORDER BY "s1"."production_year" DESC NULLS FIRST, 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t6"