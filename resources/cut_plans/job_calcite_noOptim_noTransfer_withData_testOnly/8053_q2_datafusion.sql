SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "kind" AS "COMPANY_TYPE", "keyword" AS "MOVIE_KEYWORD", "info" AS "ACTOR_INFO"
FROM "s1"
WHERE "production_year" >= 2000 AND "kind" = 'Distributor' AND "keyword" LIKE '%action%'
ORDER BY "name", "title"
FETCH NEXT 100 ROWS ONLY