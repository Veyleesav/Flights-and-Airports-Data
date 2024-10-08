# Flights-and-Airports-Data
Kaggle Flights and Airports Data

## Инструкция
1) Поднять хадуп в докере
2) docker-compose exec hive-server bash

3) `curl -L https://raw.githubusercontent.com/Veyleesav/Flights-and-Airports-Data/refs/heads/main/airports.csv > airports.csv`
4) `curl -L https://raw.githubusercontent.com/Veyleesav/Flights-and-Airports-Data/refs/heads/main/flights.csv > flights.csv`

4.1) hive

5) `hdfs dfs -mkdir /user/hive/warehouse/airports`
6) `hdfs dfs -mkdir /user/hive/warehouse/flights`


7) `hdfs dfs -put airports.csv /user/hive/warehouse/airports`
8) `hdfs dfs -put flights.csv /user/hive/warehouse/flights`

9) Подключиться к локальному хадупу, создать таблицы и витрины следующим образом

### Создаем схему и таблицы
`CREATE DATABASE otus`

### Таблица с аэропортами
`CREATE EXTERNAL TABLE otus.airports
      (airport_id int,
       city STRING,
       state STRING,
       name STRING)
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES (
       "separatorChar" = ","
      ) 
      STORED AS TEXTFILE
      LOCATION '/user/hive/warehouse/airports'
      tblproperties("skip.header.line.count"="1"
      );
`


### Таблица с полетами
`CREATE EXTERNAL TABLE otus.flights
      (dayofmonth int,
       dayofweek int,
       carrier STRING,
       originairportid int,
       destairportid int,
       depdelay int,
       arrdelay int)    
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES (
       "separatorChar" = ","
      ) 
      STORED AS TEXTFILE
      LOCATION '/user/hive/warehouse/flights'
      tblproperties("skip.header.line.count"="1"
      );`




### Базовый набор данных - можно закидывать в BI и крутить как удобно
`CREATE TABLE otus.flights_and_airports_data
AS
(
SELECT f.dayofmonth,
	   f.dayofweek,
	   f.carrier,
	   a_origin.name AS origin_airport_name,
	   a_dest.name AS destination_airport_name
FROM otus.flights AS f
LEFT JOIN otus.airports AS a_origin ON f.originairportid = a_origin.airport_id
LEFT JOIN otus.airports AS a_dest ON f.destairportid = a_dest.airport_id
)`


### Наиболее загруженные аэропорты на вылет в разбивке по дням
`CREATE TABLE otus.airports_out_pop
AS
(
SELECT f.dayofmonth,
	   a_origin.name AS origin_airport_name,
	   count(*) AS total_flights_out
FROM otus.flights AS f
LEFT JOIN otus.airports AS a_origin ON f.originairportid = a_origin.airport_id
GROUP BY f.dayofmonth,
	   a_origin.name
ORDER BY total_flights_out DESC
)`


### Наиболее загруженные аэропорты на прилет в разбивке по дням

`CREATE TABLE otus.airports_IN_pop
AS
(
SELECT f.dayofmonth,
	   a_dest.name AS destination_airport_name,
	   count(*) AS total_flights_in
FROM otus.flights AS f
LEFT JOIN otus.airports AS a_dest ON f.destairportid = a_dest.airport_id
GROUP BY f.dayofmonth,
	   a_dest.name
ORDER BY total_flights_in DESC
)`




### Топ наиболее загруженных аэропортов (на прилет и на вылет совокупно) в разбивке по дням с возможностью дополнительно посмотреть количество рейсов на прилет и на вылет в столбцах flights_in_counter и flights_out_counter соответственно
`CREATE TABLE otus.airports_traffic
AS
(
SELECT DISTINCT 
	   f.dayofmonth,
	   a_origin.airport_id AS origin_airport_id,
	   a_origin.name AS origin_airport_name,
	   a_dest.airport_id AS destination_airport_id,
	   a_dest.name AS destination_airport_name,
	   count(*) OVER (PARTITION BY f.dayofmonth, a_origin.airport_id ) AS flights_out_counter,
	   count(*) OVER (PARTITION BY f.dayofmonth, a_dest.airport_id ) AS flights_in_counter,
	   count(*) OVER (PARTITION BY f.dayofmonth, a_origin.airport_id ) + count(*) OVER (PARTITION BY f.dayofmonth, a_dest.airport_id ) AS total_flights
FROM otus.flights AS f
LEFT JOIN otus.airports AS a_origin ON f.originairportid = a_origin.airport_id
LEFT JOIN otus.airports AS a_dest ON f.destairportid = a_dest.airport_id
ORDER BY total_flights desc
)
`



### Наиболее загруженные перевозчики среди тех, кто совершил менее 100 тыс перелетов за период наблюдения
`CREATE TABLE otus.carriers_pop
AS
(
SELECT f.carrier,
       count(f.originairportid) AS flights
FROM otus.flights AS f
GROUP BY f.carrier
HAVING count(f.originairportid)<100000
ORDER BY flights DESC
)`




### Штаты, которые входят в топ-10 по количеству воздушных портов
`WITH t1 AS (SELECT state,
	   count(city) AS airport_amt,
	   RANK() OVER (ORDER BY count(city) DESC) as city_rank
FROM otus.airports
GROUP BY state)
SELECT * FROM t1
WHERE city_rank<=10
ORDER BY airport_amt DESC`
