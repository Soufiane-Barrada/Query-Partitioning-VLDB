SELECT COALESCE("t1"."name", "t1"."name") AS "name", "t1"."title", "t1"."production_year" AS "PRODUCTION_YEAR", "t1"."role", ANY_VALUE("t1"."name") AS "ACTOR_NAME", ANY_VALUE("t1"."title") AS "MOVIE_TITLE", ANY_VALUE("t1"."role") AS "ROLE_NAME", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", COUNT(DISTINCT "t1"."id0") AS "TOTAL_COACTORS"
FROM (SELECT "aka_name"."id", "aka_name"."person_id", "aka_name"."name", "aka_name"."imdb_index", "aka_name"."name_pcode_cf", "aka_name"."name_pcode_nf", "aka_name"."surname_pcode", "aka_name"."md5sum", "cast_info"."id" AS "id0", "cast_info"."person_id" AS "person_id0", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id", "t"."id" AS "id1", "t"."title", "t"."imdb_index" AS "imdb_index0", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum" AS "md5sum0", "t0"."id" AS "id2", "t0"."role", "movie_keyword"."id" AS "id3", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id"
FROM "IMDB"."movie_keyword"
RIGHT JOIN ("IMDB"."cast_info" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t" ON "cast_info"."movie_id" = "t"."id" INNER JOIN "IMDB"."aka_name" ON "cast_info"."person_id" = "aka_name"."person_id" INNER JOIN (SELECT *
FROM "IMDB"."role_type"
WHERE "role" LIKE '%Lead%') AS "t0" ON "cast_info"."role_id" = "t0"."id") ON "movie_keyword"."movie_id" = "t"."id") AS "t1"
LEFT JOIN "IMDB"."keyword" ON "t1"."keyword_id" = "keyword"."id"
GROUP BY "t1"."name", "t1"."title", "t1"."production_year", "t1"."role"