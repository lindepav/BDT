#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

r"""
 Slightly modified example from Spark repository
 Counts words in UTF8 encoded, '\n' delimited text received from the network.
 Usage: structured_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Structured Streaming
   would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
 
 spark-submit --master yarn --conf spark.sql.shuffle.partitions=2 --num-executors 2 --conf spark.ui.enabled=false spark_streaming_demo.py hador.ics.muni.cz 15015
"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, when, col, lit, split

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = (
        SparkSession
        .builder
        .appName("SST-STREAMING-FILE")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = (
        spark
        .readStream
        .format('socket')
        .option('host', host)
        .option('port', port)
        .load()
    )

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )
    
    # Transform words to lower case
    words = words.withColumn('word', lower(col('word')))

    # Read stop words as dataframe
    stop_df = spark.read.text('/user/pascepet/data/stopwords.txt').cache()

    # Create condition column
    is_stop_word = stop_df['value'].isNotNull()

    words = (
        words
        .join(stop_df, on=stop_df['value'] == words['word'], how='left')
        .select(
            when(is_stop_word, lit("stopword")).otherwise(words["word"]).alias("word")
        )
    )

    # Generate running word count
    wordCounts = words.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    query = (
        words
        .writeStream
        .trigger(processingTime='5 seconds')
        .outputMode('append')
        .format('parquet')
        .option("path", "streaming/ingest")  # put your path here
        .option("checkpointLocation", "streaming/.check")  # do not forget to add checkpoint location, can be any folder on hdfs
        .start()
    )

    query.awaitTermination()
