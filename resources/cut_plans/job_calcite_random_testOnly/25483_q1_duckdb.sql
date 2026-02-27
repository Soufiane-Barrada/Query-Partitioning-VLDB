SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."movie_id", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."note", "t"."md5sum", "movie_keyword"."id" AS "id0", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id"
FROM "IMDB"."movie_keyword"
RIGHT JOIN (SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t" ON "movie_keyword"."movie_id" = "t"."id"