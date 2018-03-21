package com.mapr.examples


import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import scala.util.Try

/*

*/
object ClickstreamConsumer {

  case class Click(user_id: Integer, datetime: String, os: String, browser: String, response_time_ms: String, product: String, url: String) extends Serializable

  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println("Usage: ClickstreamConsumer <stream:topic> ")
      System.exit(1)
    }
    val schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("datetime", StringType, true),
      StructField("os", StringType, true),
      StructField("browser", StringType, true),
      StructField("response_time_ms", StringType, true),
      StructField("product", StringType, true),
      StructField("url", StringType, true)
    ))

    val Array(topicc) = args
    val groupId = "testgroup"
    val offsetReset = "earliest"  //  "latest"
    val pollTimeout = "5000"
    val brokers = "this.will.be.ignored:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(ClickstreamConsumer.getClass.getName).setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.sparkContext.setLogLevel("ERROR")
    val topicsSet = topicc.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )

    val valuesDStream = messagesDStream.map(_.value())

    valuesDStream.foreachRDD { (rdd: RDD[String], time: Time) =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        import org.apache.spark.sql.functions._
        val df: Dataset[Click] = spark.read.schema(schema).json(rdd).as[Click]
        df.show
        df.createOrReplaceTempView("clickstream_snapshot")
        spark.sql("select count(*) from clickstream_snapshot").show
      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
