SELECT 
    p.Id AS PostId,
    p.Title,
    p.Score,
    p.CreationDate,
    p.OwnerUserId,
    p.AnswerCount,
    p.CommentCount,
    COUNT(CASE WHEN v.VoteTypeId = 2 THEN 1 END) AS UpVotes,
    COUNT(CASE WHEN v.VoteTypeId = 3 THEN 1 END) AS DownVotes
FROM Posts p
LEFT JOIN Votes v ON p.Id = v.PostId
WHERE p.CreationDate > (CAST('2024-10-01 12:34:56' AS TIMESTAMP) - INTERVAL '30 days')
GROUP BY p.Id, p.Title, p.Score, p.CreationDate, p.OwnerUserId, p.AnswerCount, p.CommentCount;