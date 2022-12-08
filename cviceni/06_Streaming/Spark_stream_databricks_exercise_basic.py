# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Streaming Databricks exercise

# COMMAND ----------

# MAGIC %md
# MAGIC When making databricks notebook, you can choose which language should be native for it (should be python for you, or if we have scala lovers go on ...), however if you want to run some cell in different language, you can do it by writing *%language* at top of the cell - eg. for sql cell, write as the first commant in cell %sql. 

# COMMAND ----------

# MAGIC %md
# MAGIC Fist make sure, you are in the right repo (repository), in which you copied (forked?) the help files (eg. *pid_schema* notebook). 
# MAGIC 
# MAGIC You can open the pid_schema notebook on another tab and take a quick look there sometimes.
# MAGIC 
# MAGIC So we are streaming data from *Pražská integrovaná doprava* from the page [www.golemio.cz](www.golemio.cz), direct link to entries is [here](https://api.golemio.cz/v2/pid/docs/openapi/#/%F0%9F%9B%A4%20RealTime%20Vehicle%20Positions/get_vehiclepositions). They just updated the webpage, feel free to look through its documentation, however it won't be much of a help.
# MAGIC 
# MAGIC We are strearimg continously (every 1 min) into 5 kafka topics, which are named: *trams, trains, buses, regbuses, boats*. It is possible, that sometimes the streams may be cut off, if so, tell teachers and it will be promptly fixed.
# MAGIC 
# MAGIC Every data input in stream consist of information about one vehicle, its location and information/specification. This means, that there may be several inputs for each vehicle!!!
# MAGIC 
# MAGIC 
# MAGIC Running the following cell will load the content of the *pid_schema* notebook, we need schema saved there. This notebook has to be in the same repo.

# COMMAND ----------

# MAGIC %run "./pid_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can for example call function *get_pid_schema* and get the schema for stream. Which was premade for this class, so you don't have to do it again. 

# COMMAND ----------

get_pid_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets try and read the tram topic. We will be reading only one topic at time for clarity, however it is possible, to read all topics simultaneously or all in one stream. Read the incode comments for better understanding of each command.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# connect to broker
JAAS = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="fel.student" password="FelBigDataWinter2022bflmpsvz";'
# at frist just plain pwd
# Subscribe to 1 topic
# name of the topic we want to suscribe too is in last option
# by adding option   .option("startingOffsets", "earliest") we can read from the beggining of the stream, try it, but probably memory won't be able to handle it
df_trains = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "trains") \
  .load()

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_trains = df_trains.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
select_stream = select_base_trains.writeStream \
        .format("memory")\
        .queryName("mem2")\
        .outputMode("append")\
        .start()

# COMMAND ----------


df_buses = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "buses") \
  .load()

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_buses = df_buses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
select_stream = select_base_buses.writeStream \
        .format("memory")\
        .queryName("mem_buses")\
        .outputMode("append")\
        .start()

# COMMAND ----------

# name of the topic we want to suscribe too is in last option
# by adding option   .option("startingOffsets", "earliest") we can read from the beggining of the stream, try it, but probably memory won't be able to handle it
df_trams = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "trams") \
  .load()

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_trams = df_trams.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
select_stream = select_base_trams.writeStream \
        .format("memory")\
        .queryName("mem")\
        .outputMode("append")\
        .start()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets make a new table, called *trams* with which we will try to make some transformation. Since creating new tables can end in error, if they already exists, it is better to delete them (if you are sure, you are not deleting others work).

# COMMAND ----------

# MAGIC %sql drop table trams

# COMMAND ----------

# MAGIC %sql
# MAGIC create table trams select * from mem

# COMMAND ----------

# MAGIC %md
# MAGIC Lets take a look on some of the the characteristics, we won't go in details much, since from the names of variables we can easily deduct, what they are. However eventhough some variables look like integers, they may be coded as strings for consistency over different type of vehicles.
# MAGIC 
# MAGIC After running next cell, you can click on the pointers to show more of the structure.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trams limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC Some of the notation is same for sql as for python, for example, you can access variables via dot notation. For example, we can try and check in the console, if the first ten entries in data are really from trams.

# COMMAND ----------

# MAGIC %sql
# MAGIC select properties.trip.vehicle_type.description_en from trams limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Task for you
# MAGIC Your task now, is to *somehow* print number of trams of each line (tram number) and sort the output from the line with largest amount of trams to the line with the smallest amount of trams.
# MAGIC There are plenty of options, how to do this. 
# MAGIC 
# MAGIC Some points which you may find helpfull:
# MAGIC  * If not sure, take a quick peek onto [https://docs.databricks.com/sql/language-manual/index.html](https://docs.databricks.com/sql/language-manual/index.html) for documentation for databricks sql or [https://www.w3schools.com/sql/sql_syntax.asp](https://www.w3schools.com/sql/sql_syntax.asp) for syntax and commands help.
# MAGIC 
# MAGIC  * Look at the variables in properties. Some may be helpfull, some are not.
# MAGIC  * Think about what you want to do and start from the elementary commands - eg. print only lines of trams, or print number of trams - maybe *count* could help
# MAGIC    * %sql select count(some_variable) from trams
# MAGIC  * Each tram may have more than one entry in streamed data, it may be necessary to deal with it - maybe with some *group by*, or *where like* or maybe *count(distinct)*
# MAGIC    * %sql select * from trams group by some_variable
# MAGIC  * There are more ways and more variables that you can use for filtering, however some may be easier and maybe some may prove to be wrong if used
# MAGIC  * You can save created output/select statement into the table with
# MAGIC    * %sql create table new_table from select properties.trip.vehicle_type.description_en from trams 
# MAGIC  * You can use select in select statements - think about the inner select as about table, which contains the selected columns 
# MAGIC    * %sql column_1, ... from (select column_x, ... from trams here_maybe_some_filtering?) here_some_more_filtering_num_2?
# MAGIC  * The outcome may be orderable in the output table. However try and order it with *order by (how?)* and maybe add *ASC or DESC* at the end
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task for you, number 2

# COMMAND ----------

# MAGIC %md 
# MAGIC In this task we will try and find all stations, in which there is possibility to change transport type in desired time window - eg. in less then 5 minutes, so we won't catch cold by standing outside. Futhermore, we are sick of travelling in trams and want to hop onto a bus. We don't care where the bus will head on from that station, as long as it will arrive in 5 minutes (or less).
# MAGIC 
# MAGIC If you look closely into the structure of the topics, you will find out, that there are no names of the stations. Only thing we got there are IDs of the station. Since we are trying to find those stations, that fit the description, we need to find their names. 
# MAGIC 
# MAGIC First we will need to load following file called *stops.json*. This file you have to download and save somewhere into the databricks! One option is to save it into DBFS FileStore. In which there are information about all stops, which we are in dire need of. File comes from *PID* database -[link](https://data.pid.cz/stops/json/stops.json). For better loading and readability some minor changes to it were performed. You can download the file from the link, however you will need to do some cleaning on it. Or you can download the file from the github profinit/BDT repo and upload it to the filestore, for which you don't have to do any more work.
# MAGIC 
# MAGIC Since the file contains more jsons, we need to use the *multiline* option. You should store the file in dbfs, so you can read it easily. 

# COMMAND ----------

df=spark.read.option("multiline","true").json("dbfs:/FileStore/stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC We can check the loaded dataframe.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC We will create sql table called stops, from which we will save values of fullName and Ids of stations to final table stopID. 

# COMMAND ----------

df.createOrReplaceTempView("stops")

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping table stopID (if it exists we need to do it, if it does not exist, it will produce error).

# COMMAND ----------

# MAGIC %sql drop table stopID

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can extract name and IDs of stations.

# COMMAND ----------

# MAGIC %sql create table stopID select fullName as name,stops[0].gtfsIds[0] as stopid from stops

# COMMAND ----------

# MAGIC %sql select * from stopID limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC We can for example check the ID for Florenc station.

# COMMAND ----------

# MAGIC %sql select * from stopID where name like "Florenc"

# COMMAND ----------

# MAGIC %md
# MAGIC We need to load data from stream for buses/trams into the tables. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table buses

# COMMAND ----------

# MAGIC %sql
# MAGIC create table buses select * from mem_buses

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from buses limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC Create another tables for manipulating the data. In these we will copy arrival times, ID of the stop, number of the line of the bus/tram, vehicle ID and vehicle type. We will make these tables for both of the vehicle types. To do by you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task for you, number three
# MAGIC At this time, there is no more tasks, however feel free to try more than was here written. You can try and upgrade the second task in sense, that the next station of the tram and of the bus is the same. It is not hard, since there is one more variable in data, that may prove good for you. All in all, feel free to try whatever you want, because more you try now, less you will need for your homeworks.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## End text
# MAGIC Some points in the end, which may prove usefull in the future (or totally wrong). 
# MAGIC 
# MAGIC You can read more than one topic in one stream. This may somehow help you in your HW, however, it should not be necessary. 
# MAGIC 
# MAGIC Saving your output into the table and not into memory maybe way better.
# MAGIC 
# MAGIC Remember, if the stream is not working, it may not be your fault, check with your peers. 
# MAGIC 
# MAGIC  #### REMEMBER TO TERMINATE YOUR CLUSTERS IF YOU WON'T BE USING THEM. WITH THE STREAM RUNNING, THE CLUSTER IS ACTIVE AND IT WON'T AUTOTURN OFF WITH INACTIVITY!!!
