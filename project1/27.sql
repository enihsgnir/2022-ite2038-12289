SELECT DISTINCT t2.name,
                qry.sum
FROM   (SELECT t.id,
               COALESCE(SUM(cp.level), 0) AS sum
        FROM   Trainer AS t
               JOIN Gym AS g
                 ON t.id = g.leader_id
               LEFT OUTER JOIN CaughtPokemon AS cp
                            ON t.id = cp.owner_id
                               AND cp.level >= 50
        GROUP  BY t.id) AS qry
       JOIN Trainer AS t2
         ON qry.id = t2.id
ORDER  BY t2.name;
