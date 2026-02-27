SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."PERSON_INFO", "t2"."PERSON_ROLE", "t2"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "s1"."title" AS "MOVIE_TITLE", "s1"."info" AS "PERSON_INFO", "role_type"."role" AS "PERSON_ROLE", "s1"."production_year"
FROM "IMDB"."role_type"
INNER JOIN "s1" ON "role_type"."id" = "s1"."role_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"