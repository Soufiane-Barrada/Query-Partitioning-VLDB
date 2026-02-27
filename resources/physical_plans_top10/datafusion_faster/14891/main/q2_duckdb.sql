WITH badge_agg AS (
    SELECT UserId, COUNT(DISTINCT Id) AS BadgeCount
    FROM Badges
    GROUP BY UserId
)
SELECT 
    p.Id AS PostId,
    p.Title,
    p.CreationDate,
    p.ViewCount,
    COALESCE(s1.CommentCount, 0) AS CommentCount,
    COALESCE(s1.VoteCount, 0) AS VoteCount,
    COALESCE(b.BadgeCount, 0) AS BadgeCount,
    u.DisplayName AS OwnerDisplayName,
    u.Reputation AS OwnerReputation,
    p.LastActivityDate
FROM Posts p
JOIN Users u ON p.OwnerUserId = u.Id
LEFT JOIN badge_agg b ON u.Id = b.UserId
LEFT JOIN s1 ON p.Id = s1.PostId
WHERE p.CreationDate >= '2022-01-01'
ORDER BY p.ViewCount DESC;