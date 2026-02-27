SELECT COALESCE("t6"."TITLE_ID", "t6"."TITLE_ID") AS "TITLE_ID", "t6"."TITLE", "t6"."PRODUCTION_YEAR", "t6"."INFO_COUNT", "t6"."LATEST_INFO", "t6"."EARLIEST_INFO", "t6"."ACTOR_COUNT", "t6"."ROLES"
FROM (SELECT "t2"."id" AS "TITLE_ID", "t2"."title" AS "TITLE", "t2"."production_year" AS "PRODUCTION_YEAR", CASE WHEN "t2"."INFO_COUNT" IS NOT NULL THEN CAST("t2"."INFO_COUNT" AS BIGINT) ELSE 0 END AS "INFO_COUNT", CASE WHEN "t2"."LATEST_INFO" IS NOT NULL THEN CAST("t2"."LATEST_INFO" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'N/A' END AS "LATEST_INFO", CASE WHEN "t2"."EARLIEST_INFO" IS NOT NULL THEN CAST("t2"."EARLIEST_INFO" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'N/A' END AS "EARLIEST_INFO", CASE WHEN "t4"."ACTOR_COUNT" IS NOT NULL THEN CAST("t4"."ACTOR_COUNT" AS BIGINT) ELSE 0 END AS "ACTOR_COUNT", CASE WHEN "t4"."ROLES" IS NOT NULL THEN CAST("t4"."ROLES" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'No roles' END AS "ROLES"
FROM (SELECT "t1"."id", "t1"."title", "t1"."imdb_index", "t1"."kind_id", "t1"."production_year", "t1"."imdb_id", "t1"."phonetic_code", "t1"."episode_of_id", "t1"."season_nr", "t1"."episode_nr", "t1"."series_years", "t1"."md5sum", "s1"."movie_id" AS "MOVIE_ID", "s1"."INFO_COUNT", "s1"."LATEST_INFO", "s1"."EARLIEST_INFO"
FROM "s1"
RIGHT JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 OR "title" ILIKE '%award%') AS "t1" ON "s1"."movie_id" = "t1"."id") AS "t2"
LEFT JOIN (SELECT "cast_info"."movie_id" AS "MOVIE_ID", COUNT(DISTINCT "cast_info"."person_id") AS "ACTOR_COUNT", LISTAGG(DISTINCT "role_type"."role", ', ') AS "ROLES"
FROM "IMDB"."role_type"
INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."person_role_id"
GROUP BY "cast_info"."movie_id") AS "t4" ON "t2"."id" = "t4"."MOVIE_ID"
ORDER BY "t2"."production_year" DESC NULLS FIRST, 7 DESC NULLS FIRST
FETCH NEXT 50 ROWS ONLY) AS "t6"