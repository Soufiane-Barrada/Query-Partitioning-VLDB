SELECT COALESCE("t"."id", "t"."id") AS "MOVIE_ID", "t"."title" AS "TITLE", "t"."production_year" AS "PRODUCTION_YEAR", "keyword"."keyword" AS "KEYWORD", ROW_NUMBER() OVER (PARTITION BY "t"."id" ORDER BY "keyword"."keyword") AS "KEYWORD_RANK"
FROM (SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t"."id" = "movie_keyword"."movie_id"