SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."PERSON_NAME", "t2"."ROLE_ID", "t2"."MOVIE_INFO", "t2"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "s1"."name0" AS "PERSON_NAME", "cast_info"."role_id" AS "ROLE_ID", "movie_info"."info" AS "MOVIE_INFO", "t0"."production_year"
FROM (SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t0"
INNER JOIN "IMDB"."movie_info" ON "t0"."id" = "movie_info"."movie_id"
INNER JOIN ("IMDB"."cast_info" INNER JOIN "s1" ON "cast_info"."person_id" = "s1"."person_id") ON "t0"."id" = "cast_info"."movie_id"
ORDER BY "t0"."production_year", "s1"."name") AS "t2"