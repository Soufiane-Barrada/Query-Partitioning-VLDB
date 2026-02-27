SELECT COALESCE("t2"."AKA_NAME", "t2"."AKA_NAME") AS "AKA_NAME", "t2"."MOVIE_TITLE", "t2"."PERSON_ROLE_ID", "t2"."KIND", "t2"."COMPANY_NAME"
FROM (SELECT "aka_name"."name" AS "AKA_NAME", "t0"."title" AS "MOVIE_TITLE", "cast_info"."person_role_id" AS "PERSON_ROLE_ID", "comp_cast_type"."kind" AS "KIND", "s1"."name" AS "COMPANY_NAME"
FROM "IMDB"."comp_cast_type"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "comp_cast_type"."id" = "cast_info"."person_role_id"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2023) AS "t0" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "cast_info"."movie_id" = "t0"."id"
ORDER BY "aka_name"."name", "t0"."title") AS "t2"