SELECT MAX(qry.count),
       ROUND(MAX(qry.count) / SUM (qry.count) * 100, 2)
FROM   (SELECT p.type,
               COUNT(p.type)
        FROM   Pokemon AS p
        GROUP  BY p.type) AS qry;
