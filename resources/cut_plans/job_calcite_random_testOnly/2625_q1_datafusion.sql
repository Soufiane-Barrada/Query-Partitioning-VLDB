SELECT COALESCE("movie_id", "movie_id") AS "movie_id", COUNT(DISTINCT "person_id") AS "ACTOR_COUNT"
FROM "IMDB"."cast_info"
GROUP BY "movie_id"
HAVING COUNT(DISTINCT "person_id") >= 5