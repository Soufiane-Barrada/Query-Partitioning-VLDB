SELECT
    p.Id AS PostId,
    p.Title,
    p.CreationDate,
    u.DisplayName AS OwnerDisplayName,
    s.CommentCount,
    s.UpVotes,
    s.DownVotes,
    s.BadgeCount
FROM s1 s
JOIN Posts p ON s.PostId = p.Id
JOIN Users u ON s.OwnerUserId = u.Id
ORDER BY p.CreationDate DESC
LIMIT 100;