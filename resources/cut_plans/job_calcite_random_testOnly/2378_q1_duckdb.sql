SELECT COALESCE("t3"."MOVIE_ID", "t3"."MOVIE_ID") AS "MOVIE_ID", "t3"."TITLE", "t3"."PRODUCTION_YEAR", "t3"."RAN", "t0"."MOVIE_ID" AS "MOVIE_ID0", "t0"."TOTAL_CAST", "t0"."CAST_NAMES"
FROM (SELECT "cast_info"."movie_id" AS "MOVIE_ID", COUNT(*) AS "TOTAL_CAST", LISTAGG("aka_name"."name", ', ') AS "CAST_NAMES"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
GROUP BY "cast_info"."movie_id") AS "t0"
RIGHT JOIN (SELECT *
FROM (SELECT "id" AS "MOVIE_ID", "title" AS "TITLE", "production_year" AS "PRODUCTION_YEAR", ROW_NUMBER() OVER (PARTITION BY "production_year" ORDER BY "id") AS "RAN"
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t2"
WHERE "PRODUCTION_YEAR" <= 2023) AS "t3" ON "t0"."MOVIE_ID" = "t3"."MOVIE_ID"
WHERE "t0"."TOTAL_CAST" IS NULL OR "t0"."TOTAL_CAST" > 5