SELECT COALESCE("t3"."AKA_ID", "t3"."AKA_ID") AS "AKA_ID", "t3"."AKA_NAME", "t3"."TITLE_ID", "t3"."MOVIE_TITLE", "t3"."PRODUCTION_YEAR", "t3"."PERSON_INFO", "t3"."CAST_KIND"
FROM (SELECT "aka_name"."id" AS "AKA_ID", "aka_name"."name" AS "AKA_NAME", "t1"."id" AS "TITLE_ID", "t1"."title" AS "MOVIE_TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", "person_info"."info" AS "PERSON_INFO", "s1"."kind" AS "CAST_KIND"
FROM "IMDB"."role_type"
INNER JOIN ("s1" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."cast_info" ON "t1"."id" = "cast_info"."movie_id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "s1"."id" = "cast_info"."person_role_id") ON "role_type"."id" = "cast_info"."role_id"
ORDER BY "t1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t3"