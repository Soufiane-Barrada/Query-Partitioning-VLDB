SELECT COALESCE("name", "name") AS "AKA_NAME", "title" AS "MOVIE_TITLE", "note" AS "CAST_NOTE", "name0" AS "COMPANY_NAME", "keyword" AS "MOVIE_KEYWORD", "role" AS "ROLE_TYPE", "info" AS "PERSON_INFO", "production_year"
FROM "s1"
WHERE "production_year" >= 2000 AND "country_code" = 'USA' AND "info_type_id" IN (SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Biography')
ORDER BY "production_year" DESC NULLS FIRST, "name"