SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum", "cast_info"."id" AS "id0", "cast_info"."person_id", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id"
FROM (SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2023) AS "t"
INNER JOIN "IMDB"."cast_info" ON "t"."id" = "cast_info"."movie_id"