SELECT COALESCE("t9"."MOVIE_TITLE", "t9"."MOVIE_TITLE") AS "MOVIE_TITLE", "t9"."ACTOR_NAME", "t9"."CAST_TYPE", "t9"."COMPANY_NAME", "t9"."MOVIE_INFO", "t9"."MOVIE_KEYWORD", "t9"."LATEST_YEAR"
FROM (SELECT "s1"."title", "s1"."name", "comp_cast_type"."kind", "s1"."name0", "t5"."info", "keyword"."keyword", ANY_VALUE("s1"."title") AS "MOVIE_TITLE", ANY_VALUE("s1"."name") AS "ACTOR_NAME", ANY_VALUE("comp_cast_type"."kind") AS "CAST_TYPE", ANY_VALUE("s1"."name0") AS "COMPANY_NAME", ANY_VALUE("t5"."info") AS "MOVIE_INFO", ANY_VALUE("keyword"."keyword") AS "MOVIE_KEYWORD", MAX("s1"."production_year") AS "LATEST_YEAR"
FROM (SELECT *
FROM "IMDB"."info_type"
WHERE "info" LIKE '%box office%') AS "t5"
INNER JOIN "IMDB"."movie_info" ON "t5"."id" = "movie_info"."info_type_id"
INNER JOIN ("IMDB"."comp_cast_type" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN "s1" ON "movie_keyword"."movie_id" = "s1"."id") ON "comp_cast_type"."id" = "s1"."person_role_id") ON "movie_info"."movie_id" = "s1"."id"
GROUP BY "t5"."info", "comp_cast_type"."kind", "keyword"."keyword", "s1"."title", "s1"."name", "s1"."name0"
ORDER BY 13 DESC NULLS FIRST, 7
FETCH NEXT 100 ROWS ONLY) AS "t9"