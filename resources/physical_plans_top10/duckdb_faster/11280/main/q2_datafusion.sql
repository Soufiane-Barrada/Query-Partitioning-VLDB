SELECT COALESCE(t2.MOVIE_TITLE, t2.MOVIE_TITLE) AS MOVIE_TITLE, t2.ACTOR_NAME, t2.PERSON_INFO, t2.CAST_TYPE, t2.MOVIE_KEYWORD, t2.production_year
FROM (SELECT s1.title AS MOVIE_TITLE, s1.name AS ACTOR_NAME, s1.info AS PERSON_INFO, s1.kind AS CAST_TYPE, keyword.keyword AS MOVIE_KEYWORD, s1.production_year
FROM movie_info
INNER JOIN (keyword INNER JOIN movie_keyword ON keyword.id = movie_keyword.keyword_id INNER JOIN s1 ON movie_keyword.movie_id = s1.id000) ON movie_info.movie_id = s1.id000
ORDER BY s1.production_year DESC NULLS FIRST, s1.name) AS t2