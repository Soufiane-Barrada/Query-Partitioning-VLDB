SELECT COALESCE("t3"."AKA_NAME", "t3"."AKA_NAME") AS "AKA_NAME", "t3"."MOVIE_TITLE", "t3"."COMPANY_NAME", "t3"."PERSON_INFO", "t3"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "s1"."title" AS "MOVIE_TITLE", "t1"."name" AS "COMPANY_NAME", "s1"."info" AS "PERSON_INFO", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t1"
INNER JOIN "s1" ON "t1"."id" = "s1"."company_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."name") AS "t3"