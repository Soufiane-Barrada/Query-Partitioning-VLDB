SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "kind" AS "CAST_TYPE", "name0" AS "COMPANY_NAME", "info" AS "MOVIE_INFO", "keyword" AS "MOVIE_KEYWORD", "production_year"
FROM "s1"
WHERE "production_year" > 2000 AND "country_code" = 'USA'
ORDER BY "name", "production_year" DESC NULLS FIRST