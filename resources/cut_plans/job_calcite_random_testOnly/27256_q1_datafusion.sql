SELECT COALESCE("movie_keyword"."movie_id", "movie_keyword"."movie_id") AS "MOVIE_ID", "keyword"."keyword", ', ' AS "FD_COL_2"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"