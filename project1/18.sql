SELECT DISTINCT t2.name,
                qry.sum
FROM   (SELECT t.id,
               SUM(cp.level)
        FROM   Trainer AS t
               JOIN Gym AS g
                 ON t.id = g.leader_id
               JOIN CaughtPokemon AS cp
                 ON t.id = cp.owner_id
        GROUP  BY t.id) AS qry
       JOIN Trainer AS t2
         ON qry.id = t2.id
ORDER  BY qry.sum DESC,
          t2.name;
