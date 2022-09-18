SELECT DISTINCT t.name,
                qry.count
FROM   Trainer AS t
       JOIN (SELECT cp.owner_id,
                    COUNT(cp.owner_id)
             FROM   CaughtPokemon AS cp
             GROUP  BY cp.owner_id) AS qry
         ON t.id = qry.owner_id
WHERE  qry.count >= 3
ORDER  BY qry.count;
