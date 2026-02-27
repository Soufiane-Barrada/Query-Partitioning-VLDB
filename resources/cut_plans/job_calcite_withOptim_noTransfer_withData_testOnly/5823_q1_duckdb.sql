SELECT COALESCE("t1"."ACTOR_ID", "t1"."ACTOR_ID") AS "ACTOR_ID", "t1"."NAME", "t1"."MOVIE_COUNT", "t1"."$f3" AS "FD_COL_3", "t4"."TITLE", "t4"."PRODUCTION_YEAR", "t4"."ACTOR_COUNT"
FROM (SELECT ANY_VALUE("aka_name"."id") AS "ACTOR_ID", "aka_name"."name" AS "NAME", COUNT(*) AS "MOVIE_COUNT", COUNT(*) > 2 AS "$f3"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
GROUP BY "aka_name"."id", "aka_name"."name"
HAVING COUNT(*) > 2) AS "t1",
(SELECT "t3"."title" AS "TITLE", "t3"."production_year" AS "PRODUCTION_YEAR", "t3"."ACTOR_COUNT"
FROM (SELECT "aka_title"."id", "aka_title"."title", "aka_title"."production_year", COUNT(DISTINCT "cast_info0"."person_id") AS "ACTOR_COUNT"
FROM "IMDB"."aka_title"
INNER JOIN "IMDB"."cast_info" AS "cast_info0" ON "aka_title"."id" = "cast_info0"."movie_id"
GROUP BY "aka_title"."id", "aka_title"."title", "aka_title"."production_year"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3") AS "t4"