WITH TopUsers AS (
    SELECT 
        u.Id,
        u.DisplayName,
        SUM(COALESCE(b.Class, 0)) AS TotalBadges,
        COUNT(DISTINCT p.Id) AS TotalPosts
    FROM Users u
    LEFT JOIN Badges b ON u.Id = b.UserId
    LEFT JOIN Posts p ON u.Id = p.OwnerUserId
    GROUP BY u.Id, u.DisplayName
    ORDER BY TotalPosts DESC, TotalBadges DESC
    LIMIT 10
)
SELECT 
    s1.Title,
    s1.CreationDate,
    u.DisplayName AS OwnerDisplayName,
    s1.Score,
    s1.UpVotes,
    s1.DownVotes,
    tu.TotalBadges,
    tu.TotalPosts
FROM s1
JOIN Users u ON s1.OwnerUserId = u.Id
JOIN TopUsers tu ON u.Id = tu.Id
ORDER BY s1.Score DESC, s1.UpVotes DESC, s1.DownVotes ASC;