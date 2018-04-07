package com.mapr.examples

import com.mapr.db.spark._
import com.mapr.db.spark.field
import com.mapr.db.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.mapr.examples.MqttConsumer.MqttRecord


/*

PURPOSE: Receive timestamps and device name from a stream, indicating when and where failures have occurred. Upon failure, this program will update all the lagging features corresponding to the failed device.
USAGE:
  `mvn package`
  copy the uber jar to your cluster
  echo "{\"timestamp\":"$(date +%s)",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:failure --broker-list this.will.be.ignored:9092
  java -cp factory-iot-tutorial-1.0-jar-with-dependencies.jar com.mapr.examples.UpdateLaggingFeatures /apps/mqtt:failure /tmp/iantest

*/
object UpdateLaggingFeatures {

  case class FailureEvent(timestamp: String,
                          deviceName: String) extends Serializable

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: UpdateLaggingFeatures <stream:topic> <table>")
      System.exit(1)
    }
    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("deviceName", StringType, nullable = false)
    ))

    val groupId = "testgroup"
    val offsetReset = "earliest"  //  "latest"
    val pollTimeout = "5000"
    val brokers = "this.will.be.ignored:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(UpdateLaggingFeatures.getClass.getName).setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    // Use this if you're working in spark-shell:
    // var ssc = new StreamingContext(sc,Seconds(5))

    val sc = ssc.sparkContext
    ssc.sparkContext.setLogLevel("ERROR")
    val topicsSet = args(0).split(",").toSet

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
    val tableName: String = args(1)
    import com.mapr.db.spark._

    valuesDStream.foreachRDD { (rdd: RDD[String]) =>
      if (!rdd.isEmpty) {
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val ds: Dataset[FailureEvent] = spark.read.schema(schema).json(rdd).as[FailureEvent]
        ds.show()

        // get the first failure time like this:
        val failure_time = ds.select("timestamp").map(r => r.getString(0)).collect()(0).toString()

        // iterate through all the timestamps like this:
        ds.select("timestamp").map(r => r.getString(0)).foreach(
          // print the timestamp
          println(_)
        )

        // Load the MQTT data table from MapR-DB JSON
        // The table schema will be inferred when we loadFromMapRDB, like this:
        val maprd = sc.loadFromMapRDB(tableName)

        // We can select rows and filter on that RDD as shown below:
        // It's probably faster to filter by where clauses (as shown below) rather than using .filter
        // because the MapR-DB connector for spark will push down where clauses to MapR-DB.
        println("Total number of rows in " + tableName + ":")
        println(maprd.map(a => {a.timestamp}).count)
        println(sc.loadFromMapRDB("/tmp/iantest").select("timestamp").count)
        println("Total number of rows with timestamp > " + failure_time + ":")
        ds.filter(a => {a.timestamp >= failure_time}).map(a => {a.timestamp}).count
        println(sc.loadFromMapRDB("/tmp/iantest").where(field("timestamp") >= failure_time).count)
        println("---done---")
      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}

