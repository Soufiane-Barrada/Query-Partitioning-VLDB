SELECT COALESCE("id", "id") AS "id", "keyword", "phonetic_code"
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%'