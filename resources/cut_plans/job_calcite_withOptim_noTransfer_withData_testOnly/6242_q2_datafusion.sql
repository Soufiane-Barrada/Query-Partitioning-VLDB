SELECT COALESCE("t3"."AKA_ID", "t3"."AKA_ID") AS "AKA_ID", "t3"."AKA_NAME", "t3"."TITLE_ID", "t3"."MOVIE_TITLE", "t3"."PRODUCTION_YEAR", "t3"."PERSON_INFO", "t3"."CAST_KIND"
FROM (SELECT "s1"."id" AS "AKA_ID", "s1"."name" AS "AKA_NAME", "s1"."id1" AS "TITLE_ID", "s1"."title" AS "MOVIE_TITLE", "s1"."production_year" AS "PRODUCTION_YEAR", "s1"."info" AS "PERSON_INFO", "t1"."kind" AS "CAST_KIND"
FROM "IMDB"."role_type"
INNER JOIN ((SELECT *
FROM "IMDB"."comp_cast_type"
WHERE "kind" = 'actor') AS "t1" INNER JOIN "s1" ON "t1"."id" = "s1"."person_role_id") ON "role_type"."id" = "s1"."role_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."name") AS "t3"