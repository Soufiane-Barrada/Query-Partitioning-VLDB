SELECT COALESCE("name", "name") AS "AKA_NAME", "title" AS "MOVIE_TITLE", "nr_order" AS "CAST_ORDER", "info" AS "PERSON_INFO", "name0" AS "COMPANY_NAME", "keyword" AS "MOVIE_KEYWORD", "role" AS "ROLE_TYPE", "production_year"
FROM "s1"
WHERE "production_year" >= 2000 AND "production_year" <= 2020 AND "country_code" = 'USA'
ORDER BY "production_year" DESC NULLS FIRST, "nr_order"
FETCH NEXT 50 ROWS ONLY