SELECT COALESCE("t2"."MOVIE_TITLE", "t2"."MOVIE_TITLE") AS "MOVIE_TITLE", "t2"."ACTOR_NAME", "t2"."ROLE_ID"
FROM (SELECT "t0"."title" AS "MOVIE_TITLE", "aka_name"."name" AS "ACTOR_NAME", "cast_info"."role_id" AS "ROLE_ID"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2023) AS "t0" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "cast_info"."movie_id" = "t0"."id"
ORDER BY "t0"."title") AS "t2"