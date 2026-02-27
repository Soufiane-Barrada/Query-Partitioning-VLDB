SELECT COALESCE("t5"."ACTOR_NAME", "t5"."ACTOR_NAME") AS "ACTOR_NAME", "t5"."MOVIE_TITLE", "t5"."PRODUCTION_YEAR", "t5"."ROLE_NAME", "t5"."KEYWORDS", "t5"."TOTAL_COACTORS", "t5"."PRODUCTION_YEAR" AS "production_year_"
FROM (SELECT "t2"."name", "t2"."title", "t2"."production_year" AS "PRODUCTION_YEAR", "t2"."role", ANY_VALUE("t2"."name") AS "ACTOR_NAME", ANY_VALUE("t2"."title") AS "MOVIE_TITLE", ANY_VALUE("t2"."role") AS "ROLE_NAME", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", COUNT(DISTINCT "t2"."id0") AS "TOTAL_COACTORS"
FROM (SELECT "aka_name"."id", "aka_name"."person_id", "aka_name"."name", "aka_name"."imdb_index", "aka_name"."name_pcode_cf", "aka_name"."name_pcode_nf", "aka_name"."surname_pcode", "aka_name"."md5sum", "cast_info"."id" AS "id0", "cast_info"."person_id" AS "person_id0", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id", "s1"."id" AS "id1", "s1"."title", "s1"."imdb_index" AS "imdb_index0", "s1"."kind_id", "s1"."production_year", "s1"."imdb_id", "s1"."phonetic_code", "s1"."episode_of_id", "s1"."season_nr", "s1"."episode_nr", "s1"."series_years", "s1"."md5sum" AS "md5sum0", "t1"."id" AS "id2", "t1"."role", "movie_keyword"."id" AS "id3", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id"
FROM "IMDB"."movie_keyword"
RIGHT JOIN ("IMDB"."cast_info" INNER JOIN "s1" ON "cast_info"."movie_id" = "s1"."id" INNER JOIN "IMDB"."aka_name" ON "cast_info"."person_id" = "aka_name"."person_id" INNER JOIN (SELECT *
FROM "IMDB"."role_type"
WHERE "role" LIKE '%Lead%') AS "t1" ON "cast_info"."role_id" = "t1"."id") ON "movie_keyword"."movie_id" = "s1"."id") AS "t2"
LEFT JOIN "IMDB"."keyword" ON "t2"."keyword_id" = "keyword"."id"
GROUP BY "t2"."name", "t2"."title", "t2"."production_year", "t2"."role"
ORDER BY 9 DESC NULLS FIRST, "t2"."production_year" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5"