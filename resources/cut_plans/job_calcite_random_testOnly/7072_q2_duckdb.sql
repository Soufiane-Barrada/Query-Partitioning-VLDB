SELECT COALESCE("t4"."ACTOR_NAME", "t4"."ACTOR_NAME") AS "ACTOR_NAME", "t4"."MOVIE_TITLE", "t4"."ROLE", "t4"."CASTING_TYPE", "t4"."COMPANY_NAME", "t4"."MOVIE_KEYWORD", "t4"."MOVIE_INFO", "t4"."ACTOR_COUNT", "t4"."production_year"
FROM (SELECT "s1"."name" AS "name0", "t1"."title", "s1"."role_id", "comp_cast_type"."kind", "company_name"."name", "t0"."keyword", "info_type"."info", "t1"."production_year", ANY_VALUE("s1"."name") AS "ACTOR_NAME", ANY_VALUE("t1"."title") AS "MOVIE_TITLE", ANY_VALUE("s1"."role_id") AS "ROLE", ANY_VALUE("comp_cast_type"."kind") AS "CASTING_TYPE", ANY_VALUE("company_name"."name") AS "COMPANY_NAME", ANY_VALUE("t0"."keyword") AS "MOVIE_KEYWORD", ANY_VALUE("info_type"."info") AS "MOVIE_INFO", COUNT(DISTINCT "s1"."id") AS "ACTOR_COUNT"
FROM "IMDB"."company_name"
INNER JOIN ("IMDB"."info_type" INNER JOIN "IMDB"."movie_info" ON "info_type"."id" = "movie_info"."info_type_id" INNER JOIN ("IMDB"."comp_cast_type" INNER JOIN "s1" ON "comp_cast_type"."id" = "s1"."person_role_id" INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%') AS "t0" INNER JOIN "IMDB"."movie_keyword" ON "t0"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t1"."id") ON "s1"."movie_id" = "t1"."id") ON "movie_info"."movie_id" = "t1"."id") ON "company_name"."id" = "movie_companies"."company_id"
GROUP BY "company_name"."name", "info_type"."info", "comp_cast_type"."kind", "s1"."name", "s1"."role_id", "t0"."keyword", "t1"."title", "t1"."production_year"
ORDER BY 16 DESC NULLS FIRST, "t1"."production_year" DESC NULLS FIRST) AS "t4"