
UPDATE 
    coffee.altitude_meters am
SET
    low = t.altitude,
    mean = t.altitude,
    high = t.altitude
FROM
    ({select}) as t
WHERE
    t.id = am.id;

UPDATE
    coffee.altitude_meters am
SET


