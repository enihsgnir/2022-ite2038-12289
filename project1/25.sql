SELECT DISTINCT t2.name,
                qry.type,
                qry.count
FROM   (SELECT t.id,
               p.type,
               COUNT(p.type)
        FROM   Trainer AS t
               JOIN CaughtPokemon AS cp
                 ON t.id = cp.owner_id
               JOIN Pokemon AS p
                 ON cp.pid = p.id
        GROUP  BY t.id,
                  p.type) AS qry
       JOIN Trainer AS t2
         ON qry.id = t2.id
ORDER  BY t2.name,
          qry.type;
