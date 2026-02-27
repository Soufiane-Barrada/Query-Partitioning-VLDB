WITH UserStats AS (
    SELECT 
        U.Id AS UserId,
        U.Reputation,
        COUNT(DISTINCT B.Id) AS BadgeCount,
        COUNT(DISTINCT P.Id) AS TotalPosts
    FROM Users U
    LEFT JOIN Badges B ON U.Id = B.UserId
    LEFT JOIN Posts P ON U.Id = P.OwnerUserId
    GROUP BY U.Id, U.Reputation
)
SELECT 
    s1.PostId,
    s1.PostTypeId,
    s1.CreationDate,
    s1.Score,
    s1.ViewCount,
    s1.CommentCount,
    s1.VoteCount,
    US.UserId,
    US.Reputation,
    US.BadgeCount,
    US.TotalPosts
FROM s1
JOIN Users U ON s1.PostTypeId = U.Id
JOIN UserStats US ON U.Id = US.UserId
ORDER BY s1.Score DESC, s1.ViewCount DESC;