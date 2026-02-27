SELECT COALESCE("MOVIE_ID", "MOVIE_ID") AS "MOVIE_ID", "TITLE", "PRODUCTION_YEAR", "KEYWORDS", "COMPANIES", "ACTORS", "ERA", "ACTOR_COUNT"
FROM (SELECT "t2"."MOVIE_ID", "t2"."TITLE", "t2"."PRODUCTION_YEAR", "t2"."KEYWORDS", "t2"."COMPANIES", "t2"."ACTORS", "t2"."ERA", COUNT(DISTINCT "t2"."ACTORS") AS "ACTOR_COUNT"
FROM (SELECT ANY_VALUE("aka_title"."id") AS "MOVIE_ID", "aka_title"."title" AS "TITLE", "aka_title"."production_year" AS "PRODUCTION_YEAR", ARRAY_AGG(DISTINCT "s1"."keyword") AS "KEYWORDS", ARRAY_AGG(DISTINCT "company_name"."name") AS "COMPANIES", ARRAY_AGG(DISTINCT "aka_name"."name") AS "ACTORS", CASE WHEN "aka_title"."production_year" < 2000 THEN 'Classic' WHEN "aka_title"."production_year" >= 2000 AND "aka_title"."production_year" <= 2010 THEN 'Modern ' ELSE 'Recent ' END AS "ERA"
FROM "IMDB"."company_name"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("s1" INNER JOIN ("IMDB"."aka_title" INNER JOIN "IMDB"."movie_companies" ON "aka_title"."id" = "movie_companies"."movie_id") ON "s1"."movie_id" = "aka_title"."id") ON "cast_info"."movie_id" = "aka_title"."id") ON "company_name"."id" = "movie_companies"."company_id"
WHERE "aka_title"."production_year" >= 2010
GROUP BY "aka_title"."id", "aka_title"."title", "aka_title"."production_year") AS "t2"
GROUP BY "t2"."MOVIE_ID", "t2"."TITLE", "t2"."PRODUCTION_YEAR", "t2"."KEYWORDS", "t2"."COMPANIES", "t2"."ACTORS", "t2"."ERA"
ORDER BY "t2"."PRODUCTION_YEAR" DESC NULLS FIRST, 8 DESC NULLS FIRST) AS "t4"