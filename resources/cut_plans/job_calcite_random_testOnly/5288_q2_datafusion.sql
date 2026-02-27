SELECT COALESCE("t7"."person_id", "t7"."person_id") AS "PERSON_ID", "t7"."ACTOR_NAME", "t7"."MOVIE_TITLE", "t7"."COMPANY_TYPE", "t7"."TOTAL_ROLES", "t7"."AVG_INFO_LENGTH"
FROM (SELECT "t5"."person_id", "t5"."name", "t5"."title", "t5"."kind", "t5"."INFO_LENGTH", ANY_VALUE("t5"."name") AS "ACTOR_NAME", ANY_VALUE("t5"."title") AS "MOVIE_TITLE", ANY_VALUE("t5"."kind") AS "COMPANY_TYPE", COUNT(DISTINCT "t3"."role_id") AS "TOTAL_ROLES", AVG("t5"."INFO_LENGTH") AS "AVG_INFO_LENGTH"
FROM (SELECT "role_id", "movie_id"
FROM "IMDB"."cast_info"
GROUP BY "movie_id", "role_id") AS "t3"
INNER JOIN (SELECT "aka_name"."id", "aka_name"."person_id", "aka_name"."name", "aka_name"."imdb_index", "aka_name"."name_pcode_cf", "aka_name"."name_pcode_nf", "aka_name"."surname_pcode", "aka_name"."md5sum", "cast_info0"."id" AS "id0", "cast_info0"."person_id" AS "person_id0", "cast_info0"."movie_id", "cast_info0"."person_role_id", "cast_info0"."note", "cast_info0"."nr_order", "cast_info0"."role_id", "t4"."id" AS "id1", "t4"."title", "t4"."imdb_index" AS "imdb_index0", "t4"."kind_id", "t4"."production_year", "t4"."imdb_id", "t4"."phonetic_code", "t4"."episode_of_id", "t4"."season_nr", "t4"."episode_nr", "t4"."series_years", "t4"."md5sum" AS "md5sum0", "movie_companies"."id" AS "id2", "movie_companies"."movie_id" AS "movie_id0", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note" AS "note0", "company_type"."id" AS "id3", "company_type"."kind", "s1"."MOVIE_ID", "s1"."INFO_LENGTH"
FROM "s1"
RIGHT JOIN ("IMDB"."cast_info" AS "cast_info0" INNER JOIN ("IMDB"."movie_companies" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t4" ON "movie_companies"."movie_id" = "t4"."id" INNER JOIN "IMDB"."company_type" ON "movie_companies"."company_type_id" = "company_type"."id") ON "cast_info0"."movie_id" = "t4"."id" INNER JOIN "IMDB"."aka_name" ON "cast_info0"."person_id" = "aka_name"."person_id") ON "s1"."MOVIE_ID" = "t4"."id") AS "t5" ON "t3"."role_id" = "t5"."role_id" AND "t3"."movie_id" = "t5"."movie_id"
GROUP BY "t5"."person_id", "t5"."name", "t5"."title", "t5"."kind", "t5"."INFO_LENGTH"
ORDER BY 9 DESC NULLS FIRST, 10 DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t7"