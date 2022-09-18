SELECT DISTINCT t.name
FROM   Trainer AS t
       JOIN Gym AS g
         ON t.id = g.leader_id
ORDER  BY t.name;
