SELECT COALESCE("movie_id", "movie_id") AS "movie_id", COUNT(*) AS "INFO_COUNT", MAX("info") AS "LATEST_INFO", MIN("info") AS "EARLIEST_INFO"
FROM "IMDB"."movie_info"
GROUP BY "movie_id"