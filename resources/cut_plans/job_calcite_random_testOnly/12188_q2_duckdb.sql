SELECT COALESCE("t3"."title", "t3"."title") AS "TITLE", "t3"."ACTOR_NAME", "t3"."production_year" AS "PRODUCTION_YEAR", "t3"."TOTAL_CAST_MEMBERS", "t3"."production_year" AS "production_year_"
FROM (SELECT "t0"."title", "s1"."name", "t0"."production_year", ANY_VALUE("s1"."name") AS "ACTOR_NAME", COUNT(*) AS "TOTAL_CAST_MEMBERS"
FROM "s1"
INNER JOIN ("IMDB"."company_name" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "company_name"."id" = "movie_companies"."company_id") ON "s1"."movie_id" = "t0"."id"
GROUP BY "s1"."name", "t0"."title", "t0"."production_year"
ORDER BY "t0"."production_year" DESC NULLS FIRST, 5 DESC NULLS FIRST) AS "t3"