SELECT COALESCE("comp_cast_type"."id", "comp_cast_type"."id") AS "id", "comp_cast_type"."kind", "cast_info"."id" AS "id0", "cast_info"."person_id", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id", "complete_cast"."id" AS "id1", "complete_cast"."movie_id" AS "movie_id0", "complete_cast"."subject_id", "complete_cast"."status_id", "t"."id" AS "id00", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum"
FROM "IMDB"."comp_cast_type"
INNER JOIN "IMDB"."cast_info" ON "comp_cast_type"."id" = "cast_info"."person_role_id"
INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t" ON "complete_cast"."movie_id" = "t"."id") ON "cast_info"."movie_id" = "t"."id"
WHERE "cast_info"."person_id" = "complete_cast"."subject_id"