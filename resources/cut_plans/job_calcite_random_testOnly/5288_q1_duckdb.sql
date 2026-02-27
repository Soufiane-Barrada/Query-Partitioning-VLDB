SELECT COALESCE("movie_id", "movie_id") AS "MOVIE_ID", AVG(LENGTH("info")) AS "INFO_LENGTH"
FROM "IMDB"."movie_info"
GROUP BY "movie_id"