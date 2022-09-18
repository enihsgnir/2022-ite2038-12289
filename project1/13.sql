SELECT DISTINCT p2.id,
                p2.name,
                p3.name,
                p4.name
FROM   (SELECT p.id        AS id1,
               e.after_id  AS id2,
               e2.after_id AS id3
        FROM   Pokemon AS p
               JOIN Evolution AS e
                 ON p.id = e.before_id
               JOIN Evolution AS e2
                 ON e.after_id = e2.before_id) AS qry
       JOIN Pokemon AS p2
         ON qry.id1 = p2.id
       JOIN Pokemon AS p3
         ON qry.id2 = p3.id
       JOIN Pokemon AS p4
         ON qry.id3 = p4.id
ORDER  BY p2.id;
