SELECT COALESCE("cast_info"."movie_id", "cast_info"."movie_id") AS "MOVIE_ID", LISTAGG("aka_name"."name", ', ') AS "CAST_NAMES", COUNT(DISTINCT "cast_info"."person_id") AS "TOTAL_CAST_COUNT"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
GROUP BY "cast_info"."movie_id"