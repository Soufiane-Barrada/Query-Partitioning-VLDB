SELECT COALESCE("t6"."ACTOR_NAME", "t6"."ACTOR_NAME") AS "ACTOR_NAME", "t6"."MOVIE_TITLE", "t6"."role_id" AS "ROLE_ID", "t6"."COMPANY_NAME", "t6"."KEYWORD_COUNT", "t6"."name"
FROM (SELECT "t3"."name", "t3"."title", "t3"."role_id", "t3"."name0", ANY_VALUE("t3"."name") AS "ACTOR_NAME", ANY_VALUE("t3"."title") AS "MOVIE_TITLE", ANY_VALUE("t3"."name0") AS "COMPANY_NAME", COUNT(DISTINCT "keyword"."keyword") AS "KEYWORD_COUNT"
FROM "IMDB"."keyword"
RIGHT JOIN (SELECT "s1"."id", "s1"."person_id", "s1"."movie_id", "s1"."person_role_id", "s1"."note", "s1"."nr_order", "s1"."role_id", "aka_name"."id" AS "id0", "aka_name"."person_id" AS "person_id0", "aka_name"."name", "aka_name"."imdb_index", "aka_name"."name_pcode_cf", "aka_name"."name_pcode_nf", "aka_name"."surname_pcode", "aka_name"."md5sum", "t2"."id" AS "id1", "t2"."movie_id" AS "movie_id0", "t2"."title", "t2"."imdb_index" AS "imdb_index0", "t2"."kind_id", "t2"."production_year", "t2"."phonetic_code", "t2"."episode_of_id", "t2"."season_nr", "t2"."episode_nr", "t2"."note" AS "note0", "t2"."md5sum" AS "md5sum0", "movie_companies"."id" AS "id2", "movie_companies"."movie_id" AS "movie_id1", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note" AS "note1", "t1"."id" AS "id3", "t1"."name" AS "name0", "t1"."country_code", "t1"."imdb_id", "t1"."name_pcode_nf" AS "name_pcode_nf0", "t1"."name_pcode_sf", "t1"."md5sum" AS "md5sum1", "movie_keyword"."id" AS "id4", "movie_keyword"."movie_id" AS "movie_id2", "movie_keyword"."keyword_id"
FROM "IMDB"."movie_keyword"
RIGHT JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t1" INNER JOIN ("IMDB"."movie_companies" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t2" INNER JOIN ("IMDB"."aka_name" INNER JOIN "s1" ON "aka_name"."person_id" = "s1"."person_id") ON "t2"."movie_id" = "s1"."movie_id") ON "movie_companies"."movie_id" = "t2"."id") ON "t1"."id" = "movie_companies"."company_id") ON "movie_keyword"."movie_id" = "t2"."id") AS "t3" ON "keyword"."id" = "t3"."keyword_id"
GROUP BY "t3"."role_id", "t3"."name", "t3"."title", "t3"."name0"
ORDER BY 8 DESC NULLS FIRST, "t3"."name") AS "t6"