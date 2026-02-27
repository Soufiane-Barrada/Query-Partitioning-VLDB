SELECT COALESCE("aka_title"."production_year", "aka_title"."production_year") AS "PRODUCTION_YEAR", COUNT(*) AS "TITLE_COUNT", AVG(LENGTH("aka_title"."title")) AS "AVG_TITLE_LENGTH", MAX(ROW_NUMBER() OVER (PARTITION BY "aka_title"."production_year" ORDER BY LENGTH("aka_title"."title") DESC NULLS FIRST)) AS "MAX_RANK", COUNT(*) > 0 AS "FD_COL_4"
FROM "IMDB"."aka_title"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "aka_title"."movie_id" = "cast_info"."movie_id"
GROUP BY "aka_title"."production_year"
HAVING COUNT(*) > 0 AND ("aka_title"."production_year" >= 2000 AND "aka_title"."production_year" <= 2020)