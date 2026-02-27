SELECT COALESCE("keyword"."id", "keyword"."id") AS "id", "keyword"."keyword", "keyword"."phonetic_code", "movie_keyword"."id" AS "id0", "movie_keyword"."movie_id", "movie_keyword"."keyword_id"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"