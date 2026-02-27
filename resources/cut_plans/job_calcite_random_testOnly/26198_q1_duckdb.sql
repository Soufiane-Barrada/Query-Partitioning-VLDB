SELECT COALESCE("title"."id", "title"."id") AS "id", "title"."title", "title"."imdb_index", "title"."kind_id", "title"."production_year", "title"."imdb_id", "title"."phonetic_code", "title"."episode_of_id", "title"."season_nr", "title"."episode_nr", "title"."series_years", "title"."md5sum", "t2"."id" AS "id0", "t2"."movie_id", "t2"."info_type_id", "t2"."info", "t2"."note"
FROM (SELECT "movie_info"."id", "movie_info"."movie_id", "movie_info"."info_type_id", "movie_info"."info", "movie_info"."note", "t0"."$f0"
FROM (SELECT SINGLE_VALUE("id") AS "$f0"
FROM "IMDB"."info_type"
WHERE "info" = 'summary') AS "t0",
"IMDB"."movie_info"
WHERE "movie_info"."info_type_id" = "t0"."$f0") AS "t2"
INNER JOIN "IMDB"."title" ON "t2"."movie_id" = "title"."id"
WHERE "title"."production_year" >= 2000 AND "title"."production_year" <= 2023