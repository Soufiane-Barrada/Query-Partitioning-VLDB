SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "PRODUCTION_YEAR", "ROLE_ID", "TOTAL_MOVIES", "name"
FROM (SELECT ANY_VALUE("name") AS "ACTOR_NAME", ANY_VALUE("title") AS "MOVIE_TITLE", "production_year" AS "PRODUCTION_YEAR", "role_id" AS "ROLE_ID", COUNT(*) AS "TOTAL_MOVIES", "name"
FROM "s1"
WHERE "production_year" >= 2000 AND "country_code" = 'USA'
GROUP BY "name", "title", "production_year", "role_id"
HAVING COUNT(*) > 1
ORDER BY 5 DESC NULLS FIRST, "name") AS "t5"