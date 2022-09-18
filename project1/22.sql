SELECT DISTINCT t2.name
FROM   (SELECT t.id
        FROM   Trainer AS t
               JOIN CaughtPokemon AS cp
                 ON t.id = cp.owner_id
               JOIN Pokemon AS p
                 ON cp.pid = p.id
        WHERE  p.type = 'Water'
        GROUP  BY t.id
        HAVING COUNT(t.id) >= 2) AS qry
       JOIN Trainer AS t2
         ON qry.id = t2.id
ORDER  BY t2.name;
