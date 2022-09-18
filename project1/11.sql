SELECT DISTINCT t2.name
FROM   (SELECT t.id,
               cp.pid
        FROM   Trainer AS t
               JOIN CaughtPokemon AS cp
                 ON t.id = cp.owner_id
        GROUP  BY t.id,
                  cp.pid
        HAVING COUNT(cp.pid) >= 2) AS qry
       JOIN Trainer AS t2
         ON qry.id = t2.id
ORDER  BY t2.name;
