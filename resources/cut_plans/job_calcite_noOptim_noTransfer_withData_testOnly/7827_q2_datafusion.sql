SELECT COALESCE("title", "title") AS "MOVIE_TITLE", "name" AS "ACTOR_NAME", "kind" AS "CAST_TYPE", "info" AS "MOVIE_INFO", "keyword" AS "MOVIE_KEYWORD", "name0" AS "COMPANY_NAME", "info0" AS "PERSON_INFO", "production_year"
FROM "s1"
WHERE "production_year" >= 2000 AND "kind" = 'Cast' AND "info_type_id" IN (SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Box Office')
ORDER BY "production_year" DESC NULLS FIRST, "name"