SELECT DISTINCT t2.name,
                qry.sum
FROM   (SELECT t.id,
               SUM(cp.level)
        FROM   Trainer AS t
               JOIN CaughtPokemon AS cp
                 ON t.id = cp.owner_id
               JOIN Pokemon AS p
                 ON cp.pid = p.id
        WHERE  t.hometown = 'Blue City'
               AND ( p.id IN (SELECT e.before_id
                              FROM   Evolution AS e)
                      OR p.id IN (SELECT e2.after_id
                                  FROM   Evolution AS e2) )
        GROUP  BY t.id) AS qry
       JOIN Trainer AS t2
         ON qry.id = t2.id
ORDER  BY t2.name;
