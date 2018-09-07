package com.vkd.twitter.config

/**
  * Created by vindev on 4/9/18.
  */

object Config {

  val ES_HOST = "127.0.0.1"
  val ES_PORT = "9200"
  val ES_INDEX_DOC = "twitter/sentiments"
  val SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  val SPARK_APP_NAME = "Twitter Sentiment Analysis"
  val SPARK_MASTER = "local[*]"
  val SPARK_CHECKPOINT_LOCATION = "tweet-streamer/data/checkpoint/offset_checkpoint"
  val AFINN_FILE_PATH = "tweet-streamer/data/AFINN.txt"
  val SAMPLE_TWEET_PATH = "tweet-streamer/data/sampleTweet.json"
  val KAFKA_SERVER = "localhost:9092"
  val KAFKA_TOPIC = "tweets"

}
