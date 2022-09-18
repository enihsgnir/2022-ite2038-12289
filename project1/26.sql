SELECT DISTINCT t3.name,
                p3.name,
                qry3.count
FROM   (SELECT t2.id AS tid,
               p2.id AS pid,
               COUNT(p2.id)
        FROM   (SELECT qry.id
                FROM   (SELECT t.id,
                               p.type
                        FROM   Trainer AS t
                               JOIN CaughtPokemon AS cp
                                 ON t.id = cp.owner_id
                               JOIN Pokemon AS p
                                 ON cp.pid = p.id
                        GROUP  BY t.id,
                                  p.type) AS qry
                GROUP  BY qry.id
                HAVING COUNT(qry.id) = 1) AS qry2
               JOIN Trainer AS t2
                 ON qry2.id = t2.id
               JOIN CaughtPokemon AS cp2
                 ON t2.id = cp2.owner_id
               JOIN Pokemon AS p2
                 ON cp2.pid = p2.id
        GROUP  BY t2.id,
                  p2.id) AS qry3
       JOIN Trainer AS t3
         ON qry3.tid = t3.id
       JOIN Pokemon AS p3
         ON qry3.pid = p3.id
ORDER  BY t3.name,
          p3.name;
