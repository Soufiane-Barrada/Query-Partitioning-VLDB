SELECT COALESCE("t"."movie_id", "t"."movie_id") AS "MOVIE_ID", "t"."ROLE_COUNT", "t0"."id", "t0"."title", "t0"."imdb_index", "t0"."kind_id", "t0"."production_year", "t0"."imdb_id", "t0"."phonetic_code", "t0"."episode_of_id", "t0"."season_nr", "t0"."episode_nr", "t0"."series_years", "t0"."md5sum", "t1"."movie_id" AS "MOVIE_ID0", "t1"."KEYWORD_COUNT"
FROM (SELECT "movie_id", COUNT(DISTINCT "person_id") AS "ROLE_COUNT"
FROM "IMDB"."cast_info"
GROUP BY "movie_id") AS "t"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t0" INNER JOIN (SELECT "movie_id", COUNT(*) AS "KEYWORD_COUNT"
FROM "IMDB"."movie_keyword"
GROUP BY "movie_id") AS "t1" ON "t0"."id" = "t1"."movie_id") ON "t"."movie_id" = "t0"."id"