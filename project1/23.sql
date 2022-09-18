SELECT DISTINCT name
FROM   Pokemon
WHERE  name LIKE 'A%'
        OR name LIKE 'E%'
        OR name LIKE 'I%'
        OR name LIKE 'O%'
        OR name LIKE 'U%'
ORDER  BY name;
