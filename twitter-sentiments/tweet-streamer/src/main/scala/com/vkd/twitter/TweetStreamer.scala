package com.vkd.twitter

import com.vkd.twitter.config.Config
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

import scala.util.{Failure, Success, Try}

/**
  * Created by vindev on 4/9/18.
  */
object TweetStreamer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .config(ConfigurationOptions.ES_NODES, Config.ES_HOST)
      .config(ConfigurationOptions.ES_PORT, Config.ES_PORT)
      .config("spark.serializer", Config.SPARK_SERIALIZER)
      .appName(Config.SPARK_APP_NAME)
      .master(Config.SPARK_MASTER)
      .getOrCreate()

    //to use all the dataframe function
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //use afinn file to create a dictionary and broadcast it
    val afinnFile = spark.read.textFile(Config.AFINN_FILE_PATH)
    val dictionary = afinnFile.map(x => {
      val line = x.split("\t")
      (line(0), line(1).toInt)
    }).collect().toMap
    val broadcastDictionary = spark.sparkContext.broadcast(dictionary)

    //to get the schema of a tweet
    val sampleTweet = spark.read.json(Config.SAMPLE_TWEET_PATH)

    //read JSON tweet from kafka
    val streamingTweets = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.KAFKA_SERVER)
      .option("subscribe", Config.KAFKA_TOPIC)
      .load()

    //parse the JSON from the coming data
    val data = streamingTweets.select($"value" cast "string" as "json")
      .select(from_json($"json", sampleTweet.schema) as "data")
      .select("data.*")

    //UDF to calculate sentiment from a tweet
    val sentimentCalculator = udf((tweet: String) => {
      Try(tweet.split(" ")) match {
        case Success(words) => words.map(broadcastDictionary.value.getOrElse(_, 0)).sum
        case Failure(_) => 0
      }
    })

    //UDF to filter out country name were state name is also present
    val getCountry = udf((str: String) => Try(str.split(",").last.trim).getOrElse("na"))

    val df = data
      .select($"text",
        $"source",
        $"user.location".as("location"),
        $"user.lang".as("language"),
        $"user.name".as("name"))
      .withColumn("text", regexp_replace(lower($"text"), "[^a-z0-9\\s+]", "")) //convert text to lowercase and remove all special chars
      .withColumn("sentiment", sentimentCalculator($"text")) //calculate the sentiment using UDF
      .withColumn("location", getCountry(lower($"location")))
      //to extract source from <a href="http://twitter.com/download/android" rel="nofollow">twitter for android</a>
      .withColumn("source", regexp_extract(lower($"source"), "(<.*\">)(\\S+.*)(<.*>)", 2))
      .select( //select only the required columns
        $"source",
        $"location",
        lower($"language").as("language"),
        $"sentiment")

    //actual query that will get executed in every batch
    val query = df
      .writeStream
      .outputMode("append")
      .format("es")
      .option("checkpointLocation", Config.SPARK_CHECKPOINT_LOCATION)
      .start(Config.ES_INDEX_DOC)


    query.awaitTermination()

  }

}
