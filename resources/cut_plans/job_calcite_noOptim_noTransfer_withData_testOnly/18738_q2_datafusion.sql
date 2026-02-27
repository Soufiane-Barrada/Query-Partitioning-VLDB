SELECT COALESCE("title", "title") AS "TITLE", "name0" AS "ACTOR_NAME", "kind" AS "CAST_TYPE", "production_year"
FROM "s1"
WHERE "country_code" = 'USA' AND "production_year" >= 2000
ORDER BY "production_year" DESC NULLS FIRST