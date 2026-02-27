SELECT
    P.Id AS PostId,
    P.PostTypeId,
    P.CreationDate,
    P.Score,
    P.ViewCount,
    COALESCE(COUNT(CASE WHEN C.Id IS NOT NULL THEN 1 END), 0) AS CommentCount,
    COALESCE(COUNT(DISTINCT V.Id), 0) AS VoteCount
FROM Posts P
LEFT JOIN Comments C ON P.Id = C.PostId
LEFT JOIN Votes V ON P.Id = V.PostId
GROUP BY P.Id, P.PostTypeId, P.CreationDate, P.Score, P.ViewCount;