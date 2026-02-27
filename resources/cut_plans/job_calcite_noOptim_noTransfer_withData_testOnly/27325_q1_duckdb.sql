SELECT COALESCE("MOVIE_ID", "MOVIE_ID") AS "MOVIE_ID", "TITLE", "PRODUCTION_YEAR", CASE WHEN "ALIASES" IS NOT NULL THEN CAST("ALIASES" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'No aliases available' END AS "ALIASES", CASE WHEN "KEYWORDS" IS NOT NULL THEN CAST("KEYWORDS" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'No keywords available' END AS "KEYWORDS", "COMPANY_COUNT", "CAST_COUNT", CASE WHEN "CAST_COUNT" > 10 THEN 'Large Cast ' WHEN "CAST_COUNT" >= 5 AND "CAST_COUNT" <= 10 THEN 'Medium Cast' ELSE 'Small Cast ' END AS "CAST_SIZE"
FROM (SELECT ANY_VALUE("aka_title"."id") AS "MOVIE_ID", "aka_title"."title" AS "TITLE", "aka_title"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "aka_name"."name", ',') AS "ALIASES", LISTAGG(DISTINCT "keyword"."keyword", ',') AS "KEYWORDS", COUNT(DISTINCT "movie_companies"."company_id") AS "COMPANY_COUNT", COUNT(DISTINCT "cast_info"."person_id") AS "CAST_COUNT"
FROM "IMDB"."aka_title"
LEFT JOIN "IMDB"."aka_name" ON "aka_title"."id" = "aka_name"."person_id"
LEFT JOIN "IMDB"."movie_keyword" ON "aka_title"."id" = "movie_keyword"."movie_id"
LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
LEFT JOIN "IMDB"."movie_companies" ON "aka_title"."id" = "movie_companies"."movie_id"
LEFT JOIN "IMDB"."cast_info" ON "aka_title"."id" = "cast_info"."movie_id"
WHERE "aka_title"."production_year" >= 2000
GROUP BY "aka_title"."id", "aka_title"."title", "aka_title"."production_year") AS "t2"
WHERE "t2"."COMPANY_COUNT" > 0