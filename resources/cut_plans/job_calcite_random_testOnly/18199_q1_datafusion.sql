SELECT COALESCE(SINGLE_VALUE("id"), SINGLE_VALUE("id")) AS "FD_COL_0"
FROM "IMDB"."info_type"
WHERE "info" = 'box office'