SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."PRODUCTION_YEAR", "t2"."CAST_COUNT", "t2"."ACTOR_NAMES"
FROM (SELECT "t"."title" AS "TITLE", "t"."production_year" AS "PRODUCTION_YEAR", COUNT(*) AS "CAST_COUNT", LISTAGG(DISTINCT "aka_name"."name", ', ') AS "ACTOR_NAMES"
FROM (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t"
INNER JOIN "IMDB"."aka_title" ON "t"."id" = "aka_title"."movie_id"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "aka_title"."movie_id" = "cast_info"."movie_id"
GROUP BY "t"."title", "t"."production_year"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"