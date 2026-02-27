SELECT COALESCE("t7"."MOVIE_ID", "t7"."MOVIE_ID") AS "MOVIE_ID", "t7"."TITLE", "t7"."PRODUCTION_YEAR", "t7"."NUM_COMPANIES", "t7"."NUM_KEYWORDS", "t7"."NUM_ACTORS"
FROM (SELECT "t5"."MOVIE_ID", "t5"."TITLE", "t5"."PRODUCTION_YEAR", "t2"."NUM_COMPANIES", "t2"."NUM_KEYWORDS", "t2"."NUM_ACTORS"
FROM (SELECT "t1"."id" AS "id00", COUNT(DISTINCT "t0"."name") AS "NUM_COMPANIES", COUNT(DISTINCT "keyword"."keyword") AS "NUM_KEYWORDS", COUNT(DISTINCT "s1"."name") AS "NUM_ACTORS"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t0" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "t0"."id" = "movie_companies"."company_id" INNER JOIN "s1" ON "t1"."id" = "s1"."movie_id") ON "movie_keyword"."movie_id" = "t1"."id"
GROUP BY "t1"."id") AS "t2"
INNER JOIN (SELECT "t4"."id" AS "MOVIE_ID", "t4"."title" AS "TITLE", "t4"."production_year" AS "PRODUCTION_YEAR", "t3"."name" AS "COMPANY_NAME", "keyword0"."keyword" AS "MOVIE_KEYWORD", "s10"."name" AS "ACTOR_NAME"
FROM "IMDB"."keyword" AS "keyword0"
INNER JOIN "IMDB"."movie_keyword" AS "movie_keyword0" ON "keyword0"."id" = "movie_keyword0"."keyword_id"
INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t3" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t4" INNER JOIN "IMDB"."movie_companies" AS "movie_companies0" ON "t4"."id" = "movie_companies0"."movie_id") ON "t3"."id" = "movie_companies0"."company_id" INNER JOIN "s1" AS "s10" ON "t4"."id" = "s10"."movie_id") ON "movie_keyword0"."movie_id" = "t4"."id") AS "t5" ON "t2"."id00" = "t5"."MOVIE_ID"
ORDER BY "t5"."PRODUCTION_YEAR" DESC NULLS FIRST, "t2"."NUM_ACTORS" DESC NULLS FIRST, "t2"."NUM_COMPANIES" DESC NULLS FIRST
FETCH NEXT 50 ROWS ONLY) AS "t7"