SELECT DISTINCT t2.name,
                t2.hometown
FROM   (SELECT qry2.tid,
               qry2.root
        FROM   (SELECT t.id   AS tid,
                       qry.root,
                       qry.id AS pid
                FROM   Trainer AS t
                       JOIN CaughtPokemon AS cp
                         ON t.id = cp.owner_id
                       JOIN (SELECT p.id,
                                    ( COALESCE(COALESCE(e2.before_id,
                                               e.before_id),
                                      p.id) ) AS
                                    root
                             FROM   Pokemon AS p
                                    LEFT OUTER JOIN Evolution AS e
                                                 ON p.id = e.after_id
                                    LEFT OUTER JOIN Evolution AS e2
                                                 ON e.before_id = e2.after_id
                             GROUP  BY root,
                                       p.id) AS qry
                         ON cp.pid = qry.id
                GROUP  BY t.id,
                          qry.root,
                          qry.id) AS qry2
        GROUP  BY qry2.tid,
                  qry2.root
        HAVING COUNT(qry2.root) >= 2) AS qry3
       JOIN Trainer AS t2
         ON qry3.tid = t2.id
ORDER  BY t2.name;
