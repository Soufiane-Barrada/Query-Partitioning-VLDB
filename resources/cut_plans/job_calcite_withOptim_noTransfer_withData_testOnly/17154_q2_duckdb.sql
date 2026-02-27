SELECT COALESCE("t4"."TITLE", "t4"."TITLE") AS "TITLE", "t4"."ACTOR_NAME", "t4"."ACTOR_INFO", "t4"."production_year"
FROM (SELECT "t2"."title" AS "TITLE", "aka_name"."name" AS "ACTOR_NAME", "person_info"."info" AS "ACTOR_INFO", "t2"."production_year"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id" INNER JOIN ("IMDB"."cast_info" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t2" ON "complete_cast"."movie_id" = "t2"."id") ON "cast_info"."id" = "complete_cast"."subject_id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "s1"."movie_id" = "t2"."id"
ORDER BY "t2"."production_year" DESC NULLS FIRST) AS "t4"