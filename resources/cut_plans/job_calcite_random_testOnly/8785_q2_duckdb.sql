SELECT COALESCE("title"."title", "title"."title") AS "TITLE", "title"."production_year" AS "PRODUCTION_YEAR", "s1"."CAST_NAMES", "t3"."KEYWORDS", CASE WHEN "movie_info"."info" IS NOT NULL THEN CAST("movie_info"."info" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'No info' END AS "MOVIE_INFO"
FROM "IMDB"."movie_info"
RIGHT JOIN ("IMDB"."title" LEFT JOIN "s1" ON "title"."id" = "s1"."MOVIE_ID" LEFT JOIN (SELECT "movie_keyword"."movie_id" AS "MOVIE_ID", LISTAGG("keyword"."keyword", ', ') AS "KEYWORDS"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
GROUP BY "movie_keyword"."movie_id") AS "t3" ON "title"."id" = "t3"."MOVIE_ID") ON "movie_info"."movie_id" = "title"."id"
WHERE "title"."production_year" >= 2000 AND "t3"."KEYWORDS" ILIKE '%action%' AND "s1"."CAST_NAMES" IS NOT NULL