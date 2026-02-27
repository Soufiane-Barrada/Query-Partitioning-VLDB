SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."ROLE_NOTE", "t2"."COMPANY_NAME"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "cast_info"."note" AS "ROLE_NOTE", "s1"."name" AS "COMPANY_NAME"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "cast_info"."movie_id" = "t0"."movie_id"
ORDER BY "t0"."title") AS "t2"