TRUNCATE handelingen;
INSERT INTO handelingen(
            id, naam, datum, kosten, fk_org, fk_spec)
    VALUES (1, 'ok', '2016-8-18 10:25:00', 123.99, 1, 1),
    (2, 'ok', '2016-8-18 10:26:00', 500, 3, 3),
    (3, 'ok heup', '2016-8-18 11:25:00', 123.99, 2, 2);

UPDATE handeling SET naam = 'ok2' where id = 1;