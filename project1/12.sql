SELECT DISTINCT t2.hometown,
                cp2.nickname
FROM   (SELECT t.hometown,
               MAX(cp.level)
        FROM   Trainer AS t
               JOIN CaughtPokemon AS cp
                 ON t.id = cp.owner_id
        GROUP  BY t.hometown) AS qry
       JOIN Trainer AS t2
         ON qry.hometown = t2.hometown
       JOIN CaughtPokemon AS cp2
         ON t2.id = cp2.owner_id
            AND qry.max = cp2.level
ORDER  BY t2.hometown;
