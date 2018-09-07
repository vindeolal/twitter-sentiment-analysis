package com.vkd.twitter.config

/**
  * Created by vindev on 5/9/18.
  */
object Config {

  val KAFKA_BROKER = "localhost:9092"
  val KAFKA_TOPIC = "tweets"
  val KAFKA_STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
  val TWEETER_TOPIC_LIST = List("Nike", "Hanuma Vihari", "#AFLDeesCats", "#Breaking")
  val TWEET_LIMIT = 1000
  val MAX_QUEUE_LENGTH = 10000
  val TWEETER_CONSUMER_KEY = "YOUR_TWEETER_CONSUMER_KEY"
  val TWEETER_CONSUMER_SECRET = "YOUR_TWEETER_CONSUMER_SECRET"
  val TWEETER_TOKEN = "YOUR_TWEETER_TOKEN"
  val TWEETER_SECRET = "YOUR_TWEETER_SECRET"

}
