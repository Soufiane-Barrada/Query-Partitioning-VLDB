SELECT COALESCE("t1"."id00", "t1"."id00") AS "MOVIE_ID", "t1"."NUM_COMPANIES", "t1"."NUM_KEYWORDS", "t1"."NUM_ACTORS", "t4"."MOVIE_ID" AS "MOVIE_ID0", "t4"."TITLE", "t4"."PRODUCTION_YEAR", "t4"."COMPANY_NAME", "t4"."MOVIE_KEYWORD", "t4"."ACTOR_NAME"
FROM (SELECT "t0"."id" AS "id00", COUNT(DISTINCT "t"."name") AS "NUM_COMPANIES", COUNT(DISTINCT "keyword"."keyword") AS "NUM_KEYWORDS", COUNT(DISTINCT "aka_name"."name") AS "NUM_ACTORS"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "t"."id" = "movie_companies"."company_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "t0"."id" = "cast_info"."movie_id") ON "movie_keyword"."movie_id" = "t0"."id"
GROUP BY "t0"."id") AS "t1"
INNER JOIN (SELECT "t3"."id" AS "MOVIE_ID", "t3"."title" AS "TITLE", "t3"."production_year" AS "PRODUCTION_YEAR", "t2"."name" AS "COMPANY_NAME", "keyword0"."keyword" AS "MOVIE_KEYWORD", "aka_name0"."name" AS "ACTOR_NAME"
FROM "IMDB"."keyword" AS "keyword0"
INNER JOIN "IMDB"."movie_keyword" AS "movie_keyword0" ON "keyword0"."id" = "movie_keyword0"."keyword_id"
INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t2" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t3" INNER JOIN "IMDB"."movie_companies" AS "movie_companies0" ON "t3"."id" = "movie_companies0"."movie_id") ON "t2"."id" = "movie_companies0"."company_id" INNER JOIN ("IMDB"."aka_name" AS "aka_name0" INNER JOIN "IMDB"."cast_info" AS "cast_info0" ON "aka_name0"."person_id" = "cast_info0"."person_id") ON "t3"."id" = "cast_info0"."movie_id") ON "movie_keyword0"."movie_id" = "t3"."id") AS "t4" ON "t1"."id00" = "t4"."MOVIE_ID"