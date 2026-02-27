SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."ACTOR_NAME", "t2"."ACTOR_INFO"
FROM (SELECT "t0"."title" AS "TITLE", "aka_name"."name" AS "ACTOR_NAME", "person_info"."info" AS "ACTOR_INFO"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id" INNER JOIN ("IMDB"."cast_info" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2020) AS "t0" INNER JOIN "IMDB"."complete_cast" ON "t0"."id" = "complete_cast"."movie_id") ON "cast_info"."person_id" = "complete_cast"."subject_id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "s1"."movie_id" = "t0"."id"
ORDER BY "t0"."title", "aka_name"."name") AS "t2"