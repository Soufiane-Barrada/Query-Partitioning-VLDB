SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."ACTOR_NAME", "t2"."ROLE", "t2"."production_year"
FROM (SELECT "t0"."title" AS "TITLE", "s1"."name" AS "ACTOR_NAME", "comp_cast_type"."kind" AS "ROLE", "t0"."production_year"
FROM "IMDB"."comp_cast_type"
INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" ON "complete_cast"."movie_id" = "t0"."id" INNER JOIN "s1" ON "complete_cast"."subject_id" = "s1"."person_id0") ON "comp_cast_type"."id" = "s1"."person_role_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST) AS "t2"