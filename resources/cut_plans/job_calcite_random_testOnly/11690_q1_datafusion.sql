SELECT COALESCE("complete_cast"."id", "complete_cast"."id") AS "id", "complete_cast"."movie_id", "complete_cast"."subject_id", "complete_cast"."status_id", "t"."id" AS "id0", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum", "keyword"."id" AS "id1", "keyword"."keyword", "keyword"."phonetic_code" AS "phonetic_code0", "movie_keyword"."id" AS "id00", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id"
FROM "IMDB"."complete_cast"
INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t" ON "complete_cast"."movie_id" = "t"."id"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t"."id" = "movie_keyword"."movie_id"