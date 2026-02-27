SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."movie_id", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."note", "t"."md5sum", "complete_cast"."id" AS "id0", "complete_cast"."movie_id" AS "movie_id0", "complete_cast"."subject_id", "complete_cast"."status_id"
FROM "IMDB"."complete_cast"
RIGHT JOIN (SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t" ON "complete_cast"."movie_id" = "t"."id"