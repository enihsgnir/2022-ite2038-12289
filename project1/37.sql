SELECT DISTINCT p.id,
                cp.nickname
FROM   CaughtPokemon AS cp
       JOIN Pokemon AS p
         ON cp.pid = p.id
       JOIN Trainer AS t
         ON cp.owner_id = t.id
WHERE  p.type = 'Water'
       AND t.hometown = 'Blue City'
ORDER  BY p.id,
          cp.nickname;
