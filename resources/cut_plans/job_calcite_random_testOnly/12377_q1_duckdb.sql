SELECT COALESCE("info_type"."id", "info_type"."id") AS "id", "info_type"."info", "movie_info"."id" AS "id0", "movie_info"."movie_id", "movie_info"."info_type_id", "movie_info"."info" AS "info0", "movie_info"."note", "complete_cast"."id" AS "id1", "complete_cast"."movie_id" AS "movie_id0", "complete_cast"."subject_id", "complete_cast"."status_id", "t"."id" AS "id00", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum"
FROM "IMDB"."info_type"
INNER JOIN "IMDB"."movie_info" ON "info_type"."id" = "movie_info"."info_type_id"
INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t" ON "complete_cast"."movie_id" = "t"."id") ON "movie_info"."movie_id" = "t"."id"