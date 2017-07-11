-- use database test_data

drop table if exists random_data;
SELECT generate_series(1,500000) AS id,
uuid() as uid,
md5(random()::text) AS text1,
md5(random()::text) AS text2,
md5(random()::text) AS text3,
md5(random()::text) AS text4,
md5(random()::text) AS text5,
md5(random()::text) AS text6,
random() AS num1,
random() AS num2,
random() AS num3 INTO random_data;

UPDATE random_data set text1 = 'update' ||id::text, num1 = random() where id BETWEEN 100000 en 110000;

-- voor de updates
-- werkt niet helaas
-- WITH SELECT * FROM random_data where id < 80000
-- UNION ALL
-- SELECT generate_series(80000,110000) AS id,
-- md5(random()::text) AS text1,
-- md5(random()::text) AS text2,
-- md5(random()::text) AS text3,
-- md5(random()::text) AS text4,
-- md5(random()::text) AS text5,
-- md5(random()::text) AS text6,
-- random() AS num1,
-- random() AS num2,
-- random() AS num3 AS q,
-- SELECT * FROM q INTO random_data2;