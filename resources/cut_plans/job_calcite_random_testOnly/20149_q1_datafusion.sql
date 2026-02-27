SELECT COALESCE("TITLE_ID", "TITLE_ID") AS "TITLE_ID", "TITLE", "PRODUCTION_YEAR", "CAST_COUNT", "VALID_CAST_COUNT", "DECADE_BUCKET"
FROM (SELECT ANY_VALUE("aka_title"."id") AS "TITLE_ID", "aka_title"."title" AS "TITLE", "aka_title"."production_year" AS "PRODUCTION_YEAR", COUNT("cast_info"."person_id") AS "CAST_COUNT", SUM(CASE WHEN "cast_info"."nr_order" IS NULL THEN 0 ELSE 1 END) AS "VALID_CAST_COUNT", NTILE(10) OVER (ORDER BY "aka_title"."production_year" DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "DECADE_BUCKET"
FROM "IMDB"."cast_info"
RIGHT JOIN "IMDB"."aka_title" ON "cast_info"."movie_id" = "aka_title"."id"
GROUP BY "aka_title"."id", "aka_title"."title", "aka_title"."production_year") AS "t1"
WHERE "t1"."PRODUCTION_YEAR" >= 1990