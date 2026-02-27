SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."NAME", "t2"."NOTE"
FROM (SELECT "t0"."title" AS "TITLE", "aka_name"."name" AS "NAME", "cast_info"."note" AS "NOTE"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2023) AS "t0" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "cast_info"."movie_id" = "t0"."id"
ORDER BY "t0"."title") AS "t2"