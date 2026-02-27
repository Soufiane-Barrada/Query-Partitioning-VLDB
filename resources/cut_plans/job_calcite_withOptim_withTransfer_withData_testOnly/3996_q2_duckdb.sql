SELECT COALESCE("t6"."TITLE_ID", "t6"."TITLE_ID") AS "TITLE_ID", "t6"."TITLE", "t6"."PRODUCTION_YEAR", "t6"."INFO_COUNT", "t6"."LATEST_INFO", "t6"."EARLIEST_INFO", "t6"."ACTOR_COUNT", "t6"."ROLES"
FROM (SELECT "t4"."id" AS "TITLE_ID", "t4"."title" AS "TITLE", "t4"."production_year" AS "PRODUCTION_YEAR", CASE WHEN "t4"."INFO_COUNT" IS NOT NULL THEN CAST("t4"."INFO_COUNT" AS BIGINT) ELSE 0 END AS "INFO_COUNT", CASE WHEN "t4"."LATEST_INFO" IS NOT NULL THEN CAST("t4"."LATEST_INFO" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'N/A' END AS "LATEST_INFO", CASE WHEN "t4"."EARLIEST_INFO" IS NOT NULL THEN CAST("t4"."EARLIEST_INFO" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'N/A' END AS "EARLIEST_INFO", CASE WHEN "s1"."ACTOR_COUNT" IS NOT NULL THEN CAST("s1"."ACTOR_COUNT" AS BIGINT) ELSE 0 END AS "ACTOR_COUNT", CASE WHEN "s1"."ROLES" IS NOT NULL THEN CAST("s1"."ROLES" AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'No roles' END AS "ROLES"
FROM (SELECT "t3"."id", "t3"."title", "t3"."imdb_index", "t3"."kind_id", "t3"."production_year", "t3"."imdb_id", "t3"."phonetic_code", "t3"."episode_of_id", "t3"."season_nr", "t3"."episode_nr", "t3"."series_years", "t3"."md5sum", "t2"."movie_id" AS "MOVIE_ID", "t2"."INFO_COUNT", "t2"."LATEST_INFO", "t2"."EARLIEST_INFO"
FROM (SELECT "movie_id", COUNT(*) AS "INFO_COUNT", MAX("info") AS "LATEST_INFO", MIN("info") AS "EARLIEST_INFO"
FROM "IMDB"."movie_info"
GROUP BY "movie_id") AS "t2"
RIGHT JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 OR "title" ILIKE '%award%') AS "t3" ON "t2"."movie_id" = "t3"."id") AS "t4"
LEFT JOIN "s1" ON "t4"."id" = "s1"."MOVIE_ID"
ORDER BY "t4"."production_year" DESC NULLS FIRST, 7 DESC NULLS FIRST
FETCH NEXT 50 ROWS ONLY) AS "t6"