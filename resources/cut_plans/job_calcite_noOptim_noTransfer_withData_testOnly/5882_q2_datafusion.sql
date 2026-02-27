SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "production_year" AS "PRODUCTION_YEAR", "kind" AS "COMPANY_TYPE", "keyword" AS "MOVIE_KEYWORD"
FROM "s1"
WHERE "production_year" >= 2000 AND "production_year" <= 2020 AND "kind" ILIKE '%production%' AND "keyword" ILIKE '%action%'
ORDER BY "production_year" DESC NULLS FIRST, "name"