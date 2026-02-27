SELECT COALESCE("movie_keyword"."movie_id", "movie_keyword"."movie_id") AS "MOVIE_ID", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
GROUP BY "movie_keyword"."movie_id"