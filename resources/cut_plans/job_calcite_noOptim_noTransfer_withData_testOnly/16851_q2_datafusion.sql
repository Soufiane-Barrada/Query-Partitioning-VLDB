SELECT COALESCE("title", "title") AS "TITLE", "name" AS "ACTOR_NAME", "info" AS "ACTOR_INFO"
FROM "s1"
WHERE "production_year" = 2023 AND "info_type_id" = (((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Biography')))
ORDER BY "title", "name"