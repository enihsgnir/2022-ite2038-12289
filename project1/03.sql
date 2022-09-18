SELECT DISTINCT t3.name,
                cp3.nickname
FROM   (SELECT t2.id,
               MAX(cp2.level)
        FROM   (SELECT t.id
                FROM   Trainer AS t
                       JOIN CaughtPokemon AS cp
                         ON t.id = cp.owner_id
                GROUP  BY t.id
                HAVING COUNT(t.id) >= 3) AS qry
               JOIN Trainer AS t2
                 ON qry.id = t2.id
               JOIN CaughtPokemon AS cp2
                 ON t2.id = cp2.owner_id
        GROUP  BY t2.id) AS qry2
       JOIN Trainer AS t3
         ON qry2.id = t3.id
       JOIN CaughtPokemon AS cp3
         ON t3.id = cp3.owner_id
WHERE  cp3.level = qry2.max
ORDER  BY t3.name,
          cp3.nickname;
