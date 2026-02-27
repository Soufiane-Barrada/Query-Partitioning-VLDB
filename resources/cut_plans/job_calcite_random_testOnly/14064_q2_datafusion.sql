SELECT COALESCE("t3"."TITLE", "t3"."TITLE") AS "TITLE", "t3"."NAME", "t3"."INFO", "t3"."production_year"
FROM (SELECT "t1"."title" AS "TITLE", "s1"."name" AS "NAME", "s1"."info" AS "INFO", "t1"."production_year"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t0"
INNER JOIN ("s1" INNER JOIN ("IMDB"."cast_info" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "cast_info"."movie_id" = "t1"."id") ON "s1"."person_id" = "cast_info"."person_id") ON "t0"."id" = "movie_companies"."company_id"
ORDER BY "t1"."production_year" DESC NULLS FIRST, "t1"."title") AS "t3"