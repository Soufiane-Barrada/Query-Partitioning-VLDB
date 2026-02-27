SELECT COALESCE("t4"."name", "t4"."name") AS "name", "t4"."gender", "t4"."MOVIE_COUNT", "t4"."MOVIES"
FROM (SELECT "s1"."name", "s1"."gender", COUNT(*) AS "MOVIE_COUNT", ARRAY_AGG(DISTINCT "t1"."title") AS "MOVIES"
FROM "IMDB"."company_type"
INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" ILIKE '%action%') AS "t0" INNER JOIN "IMDB"."movie_keyword" ON "t0"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t1"."id") ON "s1"."movie_id" = "t1"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
GROUP BY "s1"."name", "s1"."gender"
HAVING COUNT(*) > 5
ORDER BY 3 DESC NULLS FIRST) AS "t4"