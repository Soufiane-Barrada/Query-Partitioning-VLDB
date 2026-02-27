SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "nr_order" AS "ROLE_ORDER", "kind" AS "COMPANY_TYPE", "keyword" AS "MOVIE_KEYWORD", "info" AS "MOVIE_INFO", "gender" AS "ACTOR_GENDER", "production_year"
FROM "s1"
WHERE "production_year" >= 2000 AND "production_year" <= 2023
ORDER BY "production_year" DESC NULLS FIRST, "name", "nr_order"
FETCH NEXT 100 ROWS ONLY