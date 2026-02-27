SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "ROLE_ID", "COMPANY_TYPE", "TOTAL_MOVIES", "name"
FROM (SELECT ANY_VALUE("name") AS "ACTOR_NAME", ANY_VALUE("title") AS "MOVIE_TITLE", "role_id" AS "ROLE_ID", ANY_VALUE("kind") AS "COMPANY_TYPE", COUNT(*) AS "TOTAL_MOVIES", "name"
FROM "s1"
WHERE "production_year" >= 2000 AND ("kind" = 'Distributor' OR "kind" = 'Production')
GROUP BY "name", "title", "role_id", "kind"
HAVING COUNT(*) > 5
ORDER BY 5 DESC NULLS FIRST, "name"
FETCH NEXT 10 ROWS ONLY) AS "t5"