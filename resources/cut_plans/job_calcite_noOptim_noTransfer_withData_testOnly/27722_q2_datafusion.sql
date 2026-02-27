SELECT COALESCE("s1"."MOVIE_ID", "s1"."MOVIE_ID") AS "MOVIE_ID", "s1"."TITLE", "s1"."PRODUCTION_YEAR", "s1"."CAST_COUNT", "s1"."KEYWORDS", COUNT(DISTINCT "cast_info0"."person_id") AS "UNIQUE_ACTORS"
FROM "s1"
INNER JOIN "IMDB"."cast_info" AS "cast_info0" ON "s1"."subject_id" = "cast_info0"."id"
GROUP BY "s1"."MOVIE_ID", "s1"."TITLE", "s1"."PRODUCTION_YEAR", "s1"."CAST_COUNT", "s1"."KEYWORDS"
HAVING COUNT(DISTINCT "cast_info0"."person_id") > 5