# Databricks notebook source
#Carico il dataset da DBFS
df = spark.read.csv('/FileStore/tables/videogames-2.csv', header=True, inferSchema=True)

display(df.limit(4))


# COMMAND ----------

from pyspark.sql.functions import col, count, when

# Conta i valori NULL in ogni colonna
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

# Conta il numero totale di valori per ogni colonna
total_counts = df.select([count(col(c)).alias(c) for c in df.columns])

# Mostra il conteggio dei valori nulli e totali per ogni colonna
total_counts.show()
null_counts.show()




# COMMAND ----------

# Elimina le righe con valori nulli
df_cleaned = df.dropna()

# Mostra il DataFrame pulito
display(df_cleaned.limit(4))


# COMMAND ----------

from pyspark.sql.functions import col

# Raggruppa i dati per titolo del gioco e conta il numero di occorrenze per ciascun titolo
conteggio_giochi = df_cleaned.groupBy("Game Title").count()

# Ordina i risultati in ordine decrescente in base al conteggio
giochi_ord = conteggio_giochi.orderBy(col("count").desc())

# Mostra i risultati ordinati
display(giochi_ord)


# COMMAND ----------

from pyspark.sql.functions import col, avg

# media vendite globali per genere
media_genere = df_cleaned.groupBy("Genre").agg(avg("Global").alias("Avg_Global_Sales"))

# ordina in ordine decrescente
media_gen_ord = media_genere.orderBy(col("Avg_Global_Sales").desc())

# Mostra i risultati ordinati
display(media_gen_ord.limit(4))





# COMMAND ----------

from pyspark.sql.functions import col

# Filtra i giochi per i generi "Platform", "Role-Playing" e "Misc"
filtered_games = df_cleaned.filter(col("Genre").isin("Platform", "Role-Playing", "Misc"))

# Raggruppa i dati per publisher e genere, quindi conta il numero di giochi per ciascun publisher e genere
conteggio_publisher = filtered_games.groupBy("Publisher", "Genre").count()

# Ordina i risultati in ordine decrescente in base al conteggio
conteggio_publisher_ord = conteggio_publisher.orderBy(col("count").desc())

# Mostra i risultati ordinati
display(conteggio_publisher_ord.limit(10))






# COMMAND ----------

from pyspark.sql.functions import col

# Filtra i giochi per il genere "Platform"
platform_games = df_cleaned.filter(col("Genre") == "Platform")

# Raggruppa i dati per publisher e conta il numero di giochi di genere "Platform" per ciascun publisher
publisher_platform_counts = platform_games.groupBy("Publisher").count()

# Ordina i risultati in ordine decrescente in base al conteggio
publisher_platform_counts_ordered = publisher_platform_counts.orderBy(col("count").desc())

# Mostra i risultati ordinati
publisher_platform_counts_ordered.show()


# COMMAND ----------

from pyspark.sql.functions import col, avg

# Filtra i giochi di genere "Platform" pubblicati da Nintendo
nintendo_platform = df_cleaned.filter((col("Publisher") == "Nintendo") & (col("Genre") == "Platform"))

# Calcola la media delle vendite globali per i giochi di genere "Platform" pubblicati da Nintendo
avg_nintendo_platform = nintendo_platform.agg(avg("Global").alias("Avg_Global_Sales"))

# Mostra la media delle vendite globali
display(avg_nintendo_platform)


# COMMAND ----------

from pyspark.sql.functions import col, avg

# Filtra i giochi pubblicati da Nintendo
nintendo_games = df_cleaned.filter(col("Publisher") == "Nintendo")

# Raggruppa i dati per anno e calcola la media delle vendite globali per ciascun anno
avg_vendite_nintendo = nintendo_games.groupBy("Year").agg(avg("Global").alias("AVG_Annuale"))

# Ordina i risultati in ordine decrescente in base alla media delle vendite globali annue
avg_vendite_nintendo_ord = avg_vendite_nintendo.orderBy(col("AVG_Annuale").desc())


# Mostra i risultati ordinati
display(avg_vendite_nintendo_ord.limit(5))



# COMMAND ----------

#Giochi con una valutazione superiore all'8 e vendite globali superiori a 20milioni
df_cleaned.filter((df_cleaned["Review"] > 8) & (df_cleaned["Global"] > 20)) \
          .select("Game Title", "Review", "Global") \
          .orderBy(col("Global").desc()) \
          .show()



# COMMAND ----------

from pyspark.sql.functions import col

df_cleaned.filter((df_cleaned["Review"] > 8) & (df_cleaned["Global"] > 20)) \
          .select("Game Title", "Review", "Global", "Year") \
          .orderBy(col("Global").desc()) \
          .limit(10) \
          .show()


# COMMAND ----------

#Contare i giochi per piattaforme e anno di rilascio
df_cleaned.groupBy("Platform", "Year").count().orderBy(col("count").desc()).show()


# COMMAND ----------

from pyspark.sql.functions import avg

# Calcolare la media delle vendite per ciascuna regione
average_sales = df_cleaned.select(
    avg("North America").alias("Avg_NA_Sales"),
    avg("Europe").alias("Avg_EU_Sales"),
    avg("Japan").alias("Avg_JP_Sales"),
    avg("Rest of World").alias("Avg_RW_Sales")
)

# Mostra la media delle vendite per ciascuna regione
display(average_sales)




# COMMAND ----------

from pyspark.sql.functions import col, sum

# Raggruppa i dati per publisher e calcola la somma delle vendite per ciascuna regione
vendite_tot_publisher = df_cleaned.groupBy("Publisher").agg(
    sum("North America").alias("Vendite_NA"),
    sum("Europe").alias("Vendite_EU"),
    sum("Japan").alias("Vendite_JP"),
    sum("Rest of World").alias("Vendite_RW"),
    sum("Global").alias("Vendite_Globali")
)

# Ordina i risultati in ordine decrescente in base alla somma delle vendite globali
vendite_tot_publisher_ord = vendite_tot_publisher.orderBy(col("Vendite_Globali").desc())

# Mostra i risultati ordinati
#vendite_tot_publisher_ord.show()
display(vendite_tot_publisher_ord)


# COMMAND ----------

from pyspark.sql.functions import col

# Filtra i giochi pubblicati da Sony Computer Entertainment
sony_games = df_cleaned.filter(col("Publisher") == "Sony Computer Entertainment")

# Trova i giochi più venduti di Sony Computer Entertainment
top_sony_games = sony_games.orderBy(col("Global").desc()).select("Game Title", "Global", "Platform").show()


# COMMAND ----------

from pyspark.sql.functions import col

# Filtra i giochi pubblicati da Sony Computer Entertainment con una recensione maggiore di 20
sony_high_review_games = df_cleaned.filter((col("Publisher") == "Sony Computer Entertainment") & (col("Review") > 20))

# Ordina i giochi filtrati in base al valore di review
sony_high_review_games_ordered = sony_high_review_games.orderBy(col("Review").desc())

# Mostra i giochi filtrati e ordinati
sony_high_review_games_ordered.select("Game Title", "Review", "Global").show()


