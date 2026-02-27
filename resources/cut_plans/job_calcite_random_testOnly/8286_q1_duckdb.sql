SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."keyword", "t"."phonetic_code", "t0"."id" AS "id0", "t0"."title", "t0"."imdb_index", "t0"."kind_id", "t0"."production_year", "t0"."imdb_id", "t0"."phonetic_code" AS "phonetic_code0", "t0"."episode_of_id", "t0"."season_nr", "t0"."episode_nr", "t0"."series_years", "t0"."md5sum", "movie_keyword"."id" AS "id00", "movie_keyword"."movie_id", "movie_keyword"."keyword_id"
FROM (SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE 'action%') AS "t"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "IMDB"."movie_keyword" ON "t0"."id" = "movie_keyword"."movie_id") ON "t"."id" = "movie_keyword"."keyword_id"