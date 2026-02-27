SELECT COALESCE("title", "title") AS "MOVIE_TITLE", "name" AS "ACTOR_NAME", "kind" AS "COMPANY_TYPE", "keyword" AS "MOVIE_KEYWORD", "info" AS "PERSON_INFO", "production_year"
FROM "s1"
WHERE "production_year" > 2000 AND "keyword" LIKE '%action%' AND "kind" = 'Production'
ORDER BY "production_year" DESC NULLS FIRST, "name"