SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."PRODUCTION_YEAR", "t3"."KEYWORDS", "t3"."CAST_TYPE", "t3"."CAST_NOTE", "t3"."PRODUCTION_YEAR" AS "production_year_"
FROM (SELECT "s1"."name", "t0"."title", "t0"."production_year" AS "PRODUCTION_YEAR", "comp_cast_type"."kind", "s1"."note", ANY_VALUE("s1"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", ANY_VALUE("comp_cast_type"."kind") AS "CAST_TYPE", ANY_VALUE("s1"."note") AS "CAST_NOTE"
FROM "IMDB"."comp_cast_type"
INNER JOIN "s1" ON "comp_cast_type"."id" = "s1"."role_id"
INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t0"."id" = "movie_keyword"."movie_id") ON "s1"."movie_id" = "t0"."id"
GROUP BY "s1"."name", "t0"."title", "t0"."production_year", "comp_cast_type"."kind", "s1"."note"
ORDER BY "t0"."production_year" DESC NULLS FIRST, 6) AS "t3"