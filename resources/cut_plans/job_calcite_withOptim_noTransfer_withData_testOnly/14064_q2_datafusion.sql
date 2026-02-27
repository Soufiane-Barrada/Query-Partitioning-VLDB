SELECT COALESCE("t3"."TITLE", "t3"."TITLE") AS "TITLE", "t3"."NAME", "t3"."INFO", "t3"."production_year"
FROM (SELECT "s1"."title" AS "TITLE", "s1"."name" AS "NAME", "s1"."info" AS "INFO", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t1"
INNER JOIN "s1" ON "t1"."id" = "s1"."company_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."title") AS "t3"