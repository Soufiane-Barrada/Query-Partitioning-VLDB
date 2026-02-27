SELECT COALESCE("cast_info"."id", "cast_info"."id") AS "id", "cast_info"."person_id", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id", "movie_keyword"."id" AS "id0", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id", "t"."id" AS "id00", "t"."movie_id" AS "movie_id00", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."note" AS "note0", "t"."md5sum"
FROM "IMDB"."cast_info"
INNER JOIN ("IMDB"."movie_keyword" RIGHT JOIN (SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t" ON "movie_keyword"."movie_id" = "t"."id") ON "cast_info"."movie_id" = "t"."id"