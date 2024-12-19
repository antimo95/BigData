-- Databricks notebook source
-- MAGIC %md
-- MAGIC Dataset Videogames

-- COMMAND ----------

DESCRIBE videogames_4_csv

-- COMMAND ----------

SELECT `Game Title` AS game_name, COUNT(`Game Title`) AS game_count
FROM videogames_4_csv
WHERE `Game Title` IS NOT NULL 
GROUP BY `Game Title`
ORDER BY game_count DESC;

-- COMMAND ----------

SELECT Genre, AVG(GLOBAL) AS AVG_Globale
FROM videogames_4_csv
WHERE GLOBAL IS NOT NULL AND Genre IS NOT NULL
GROUP BY Genre
ORDER BY AVG_Globale DESC;

-- COMMAND ----------

SELECT Genre, Publisher, COUNT(*) as Genre_game_count
FROM videogames_4_csv
WHERE Genre IN ('Platform' , 'Role-Playing', 'Shooter') 
      AND 
      Genre IS NOT NULL 
      AND 
      Publisher IS NOT NULL 
GROUP BY Publisher, Genre 
ORDER BY Genre_game_count DESC 

-- COMMAND ----------

SELECT Publisher, Genre, AVG(Global) AS AVG_Vendite_Globali
FROM videogames_4_csv
WHERE Publisher = 'Nintendo'
    AND 
    Genre = 'Platform'
    AND 
    Global IS NOT NULL
    AND 
    Publisher IS NOT NULL
    AND 
    Genre IS NOT NULL
GROUP BY Publisher, Genre;

-- COMMAND ----------

SELECT Publisher, Year, AVG(Global) AS AVG_Vendite_Globali_Annue 
FROM videogames_4_csv
WHERE Publisher = 'Nintendo'
      AND 
      Global IS NOT NULL 
GROUP BY Year, Publisher 
ORDER BY AVG_Vendite_Globali_Annue DESC 

-- COMMAND ----------

SELECT `Game Title`, Review, Global 
FROM videogames_4_csv
WHERE Review > 8 
      AND 
      Global > 20 
      AND 
      Review IS NOT NULL 
      AND 
      Global IS NOT NULL 

ORDER BY Global DESC 


-- COMMAND ----------

SELECT `Game Title`, Year, Review, Global 
FROM videogames_4_csv
WHERE Review > 8 
      AND 
      Global > 20 
      AND 
      Review IS NOT NULL 
      AND 
      Global IS NOT NULL 

ORDER BY Global DESC 
LIMIT 10 

-- COMMAND ----------

SELECT `Game Title`, Publisher, Year, Global
FROM videogames_4_csv
WHERE Publisher = "Nintendo" 
      AND 
      Year = "2006"
ORDER BY Global DESC 

-- COMMAND ----------

SELECT Platform, Year, COUNT(*) AS Giochi_Annui_Piattaforma 
FROM videogames_4_csv
GROUP BY Platform, Year 
ORDER BY Giochi_Annui_Piattaforma DESC 

-- COMMAND ----------

SELECT 
    Publisher, 
    SUM(`North America`) AS Vendite_NA, 
    SUM(Europe) AS Vendite_EU, 
    SUM(Japan) AS Vendite_JP, 
    SUM(`Rest of World`) AS Vendite_RW, 
    SUM(Global) AS Vendite_Globali
FROM videogames_4_csv
GROUP BY Publisher
ORDER BY Vendite_Globali DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC NETFLIX DATASET 

-- COMMAND ----------

-- Visualizzo il dataset sottoforma di tabella 
SELECT * FROM netflix_titles_2_csv;

-- COMMAND ----------

--Schema del dataset in esame 
DESCRIBE netflix_titles_2_csv; 

-- COMMAND ----------

--Analisi Geografica 
SELECT country, COUNT(*) AS count 
FROM netflix_titles_2_csv
GROUP BY country;

-- COMMAND ----------

--Analisi temporale 
SELECT release_year, COUNT(*) AS count
FROM netflix_titles_2_csv
GROUP BY release_year
ORDER BY count DESC;


-- COMMAND ----------

--Analisi valutazioni
SELECT rating, COUNT(*) AS count
FROM netflix_titles_2_csv
GROUP BY rating;


-- COMMAND ----------

--Numero di spettacoli per attore 
SELECT actor, COUNT(*) AS count
FROM netflix_titles_2_csv
LATERAL VIEW explode(split(cast, ',')) actorTable AS actor
GROUP BY actor
ORDER BY count DESC;


-- COMMAND ----------

select * 
from movies_csv
limit 5;

-- COMMAND ----------

SELECT 
    SUM(CASE WHEN Release_Date IS NULL THEN 1 ELSE 0 END) AS Null_Release_Date,
    SUM(CASE WHEN Title IS NULL THEN 1 ELSE 0 END) AS Null_Title,
    SUM(CASE WHEN Overview IS NULL THEN 1 ELSE 0 END) AS Null_Overview,
    SUM(CASE WHEN Popularity IS NULL THEN 1 ELSE 0 END) AS Null_Popularity,
    SUM(CASE WHEN Vote_Count IS NULL THEN 1 ELSE 0 END) AS Null_Vote_Count,
    SUM(CASE WHEN Vote_Average IS NULL THEN 1 ELSE 0 END) AS Null_Vote_Average,
    SUM(CASE WHEN Original_Language IS NULL THEN 1 ELSE 0 END) AS Null_Original_Language,
    SUM(CASE WHEN Genre IS NULL THEN 1 ELSE 0 END) AS Null_Genre,
    SUM(CASE WHEN Poster_Url IS NULL THEN 1 ELSE 0 END) AS Null_Poster_Url
FROM movies_csv;


-- COMMAND ----------

--Numero film per genere 
SELECT Genre, COUNT(*) AS Count 
FROM movies_csv
WHERE Genre IS NOT NULL 
GROUP BY Genre 
ORDER BY Count DESC

-- COMMAND ----------

--media voti per genere 
SELECT AVG(Vote_Average) AS MEDIA, Genre 
FROM movies_3_csv
WHERE Vote_Average is NOT NULL 
GROUP BY Genre
ORDER BY MEDIA DESC


-- COMMAND ----------

SELECT Title, Popularity 
FROM movies_3_csv
WHERE Popularity IS NOT NULL 
ORDER BY Popularity DESC
LIMIT 10;

-- COMMAND ----------

SELECT Title, Vote_Average
FROM movies_3_csv 
WHERE Vote_Average IS NOT NULL 
ORDER BY Vote_Average DESC 
LIMIT 10
