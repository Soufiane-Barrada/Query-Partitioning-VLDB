SELECT COALESCE("t8"."AKA_NAME", "t8"."AKA_NAME") AS "AKA_NAME", "t8"."MOVIE_TITLE", "t8"."CAST_ORDER", "t8"."PERSON_INFO", "t8"."COMPANY_NAME", "t8"."MOVIE_KEYWORD", "t8"."ROLE_TYPE", "t8"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "s1"."title" AS "MOVIE_TITLE", "s1"."nr_order" AS "CAST_ORDER", "s1"."info" AS "PERSON_INFO", "t6"."name" AS "COMPANY_NAME", "s1"."keyword" AS "MOVIE_KEYWORD", "s1"."role" AS "ROLE_TYPE", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t6"
INNER JOIN "s1" ON "t6"."id" = "s1"."company_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."nr_order"
FETCH NEXT 50 ROWS ONLY) AS "t8"