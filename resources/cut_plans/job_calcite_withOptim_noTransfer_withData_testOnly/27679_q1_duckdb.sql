SELECT COALESCE("t0"."MOVIE_ID", "t0"."MOVIE_ID") AS "MOVIE_ID", "t0"."KEYWORDS", "t4"."MOVIE_ID" AS "MOVIE_ID0", "t4"."TITLE", "t4"."PRODUCTION_YEAR", "t4"."CAST_COUNT", "t4"."CAST_NAMES"
FROM (SELECT "movie_keyword"."movie_id" AS "MOVIE_ID", LISTAGG("keyword"."keyword", ', ') AS "KEYWORDS"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
GROUP BY "movie_keyword"."movie_id") AS "t0"
RIGHT JOIN (SELECT ANY_VALUE("t1"."id") AS "MOVIE_ID", "t1"."title" AS "TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", COUNT(DISTINCT "cast_info"."person_id") AS "CAST_COUNT", LISTAGG(DISTINCT "aka_name"."name", ', ') AS "CAST_NAMES"
FROM (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t1"
INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "movie_companies"."movie_id" = "cast_info"."movie_id"
GROUP BY "t1"."id", "t1"."title", "t1"."production_year") AS "t4" ON "t0"."MOVIE_ID" = "t4"."MOVIE_ID"
WHERE "t4"."CAST_COUNT" > 5