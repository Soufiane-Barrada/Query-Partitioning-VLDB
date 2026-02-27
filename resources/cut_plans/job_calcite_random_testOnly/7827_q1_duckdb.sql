SELECT COALESCE("complete_cast"."id", "complete_cast"."id") AS "id", "complete_cast"."movie_id", "complete_cast"."subject_id", "complete_cast"."status_id", "t"."id" AS "id0", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum"
FROM "IMDB"."complete_cast"
INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t" ON "complete_cast"."movie_id" = "t"."id"