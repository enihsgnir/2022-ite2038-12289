SELECT DISTINCT t2.name,
                qry.avg
FROM   (SELECT t.id,
               AVG(cp.level)
        FROM   Trainer AS t
               JOIN CaughtPokemon AS cp
                 ON t.id = cp.owner_id
               JOIN Pokemon AS p
                 ON cp.pid = p.id
        WHERE  p.type = 'Normal'
                OR p.type = 'Electric'
        GROUP  BY t.id) AS qry
       JOIN Trainer AS t2
         ON qry.id = t2.id
ORDER  BY qry.avg,
          t2.name;
