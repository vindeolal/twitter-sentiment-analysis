package com.vkd.twitter

import java.util.concurrent.LinkedBlockingQueue

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import com.vkd.twitter.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by vindev on 5/9/18.
  */
object TweetProducer {

  def main(args: Array[String]): Unit = {
    run(Config.TWEETER_CONSUMER_KEY, Config.TWEETER_CONSUMER_SECRET, Config.TWEETER_TOKEN, Config.TWEETER_SECRET)
  }

  def run(consumerKey: String, consumerSecret: String, token: String, secret: String): Unit = {
    val config = Map(
      "bootstrap.servers" -> Config.KAFKA_BROKER,
      "key.serializer" -> Config.KAFKA_STRING_SERIALIZER,
      "value.serializer" -> Config.KAFKA_STRING_SERIALIZER)
    val producer = new KafkaProducer[String, String](config)
    val queue = new LinkedBlockingQueue[String](Config.MAX_QUEUE_LENGTH)
    val endpoint = new StatusesFilterEndpoint
    endpoint.trackTerms(Config.TWEETER_TOPIC_LIST)
    val auth = new OAuth1(consumerKey, consumerSecret, token, secret)
    val client = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build
    client.connect()

    var count = 0
    println("sending tweets to kafka")
    (1 to Config.TWEET_LIMIT).foreach(x => {
      Try(new ProducerRecord[String, String](Config.KAFKA_TOPIC, queue.take)) match {
        case Success(msg) => producer.send(msg)
          count += 1
        case Failure(exception) => println(exception.printStackTrace())
      }
    })

    println(s"Total tweets sent to kafka : $count")
    producer.close()
    client.stop()
    queue.clear()
  }

}
