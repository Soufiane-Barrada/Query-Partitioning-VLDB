SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "nr_order" AS "CAST_ORDER", "info" AS "ACTOR_INFO", "keyword" AS "MOVIE_KEYWORD", "name0" AS "COMPANY_NAME", "kind" AS "COMPANY_TYPE", "production_year"
FROM "s1"
WHERE "production_year" >= 2000 AND "country_code" = 'USA' AND "kind" = 'Distributor'
ORDER BY "production_year" DESC NULLS FIRST, "name"