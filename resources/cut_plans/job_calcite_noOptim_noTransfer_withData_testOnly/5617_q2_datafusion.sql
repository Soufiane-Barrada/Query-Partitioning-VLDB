SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "kind" AS "COMPANY_TYPE", "keyword" AS "KEYWORD", "info" AS "ADDITIONAL_INFO"
FROM "s1"
WHERE "production_year" >= 2000 AND "nr_order" < 5
ORDER BY "name", "title", "kind"
FETCH NEXT 100 ROWS ONLY