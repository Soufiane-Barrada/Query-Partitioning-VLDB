SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."movie_id", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."note", "t"."md5sum", "keyword"."id" AS "id0", "keyword"."keyword", "keyword"."phonetic_code" AS "phonetic_code0", "movie_keyword"."id" AS "id00", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id"
FROM (SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t"."id" = "movie_keyword"."movie_id"