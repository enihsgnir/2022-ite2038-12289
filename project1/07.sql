SELECT DISTINCT t.name,
                qry.sum
FROM   Trainer AS t
       JOIN (SELECT cp.owner_id,
                    SUM(cp.level)
             FROM   CaughtPokemon AS cp
             GROUP  BY cp.owner_id) AS qry
         ON t.id = qry.owner_id
WHERE  qry.sum = (SELECT MAX(qry2.sum)
                  FROM   (SELECT cp2.owner_id,
                                 SUM(cp2.level)
                          FROM   CaughtPokemon AS cp2
                          GROUP  BY cp2.owner_id) AS qry2)
ORDER  BY t.name;
