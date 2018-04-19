package com.mapr.examples

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.sql.SparkSession
import com.mapr.db.spark.sql._
import org.apache.spark.sql.functions._
import com.mapr.db.spark._
import com.mapr.db.spark.field


/******************************************************************************
  PURPOSE: Receive timestamps and device name from a stream, indicating when and where failures have occurred. Upon failure, this program will update all the lagging features corresponding to the failed device.

  BUILD:
    `mvn package`
    copy target/lib to your cluster

  SYNTHESIZE DATA:

    echo "{\"timestamp\":"$(date +%s)",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:failures --broker-list this.will.be.ignored:9092

  RUN:

 /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures target/factory-iot-tutorial-1.0-jar-with-dependencies.jar <stream:topic>[,<stream2:topic2>] <tableName>

  EXAMPLE:

  /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/mqtt:failures /apps/mqtt_records

  ****************************************************************************/

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

    valuesDStream.foreachRDD { (rdd: RDD[String]) =>
      if (!rdd.isEmpty) {
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val ds: Dataset[FailureEvent] = spark.read.schema(schema).json(rdd).as[FailureEvent]
        println("Failure events received from stream: " + args(0) + ":")
        ds.show()


        // TODO: iterate on each reported failure, not just the first one.
        // iterate through all the timestamps like this:
//        ds.select("timestamp").map(r => r.getString(0)).foreach(
//          println(_)
//        )

        // get the first failure time:
        val failure_time = ds.select("timestamp").map(r => r.getString(0)).collect()(0).toString()
        // get the corresponding failed device:
        val deviceName = ds.select("deviceName").map(r => r.getString(0)).collect()(0).toString()

        // Load the MQTT data table from MapR-DB JSON
        // The table schema will be inferred when we loadFromMapRDB:
        val mqtt_rdd = sc.loadFromMapRDB(tableName)

        // We can select rows and filter on that RDD as shown below:
        println("Total number of records in table " + tableName + ":")
        println(mqtt_rdd.map(a => {a.timestamp}).count)
//        println(ds.filter(a => {a.timestamp >= failure_time}).map(a => {a.timestamp}).count)

        //mqtt_rdd.toDF().rdd.map { row => (row.getAs("timestamp").toString)}.take(3)

        // Mark the device as "about to fail" for a period of time leading up to failure_time
        val failure_window = "20"  // in seconds, to match Unix time given by `date +%s`
        val failure_imminent = (failure_time.toInt - failure_window.toInt).toString

        // For more information about how to specify filter conditions with MapR-DB, see:
        // https://maprdocs.mapr.com/home/Spark/ScalaDSLforSpecifyingPredicates.html

        val rows_to_update = mqtt_rdd.where(field("timestamp") >= failure_imminent and field("timestamp") <= failure_time)
        println("Number of samples recorded while failure was imminent (from t="+failure_imminent+" to t="+failure_time+")\n" + rows_to_update.count)
        // another way of doing the same thing:
        val mqtt_df = mqtt_rdd.toDF()
        // "AboutToFail" is a binary lagging feature intended to be used to classify whether failure is imminent
        val binary_lagging_feature = mqtt_df.filter(mqtt_df("timestamp") >= failure_imminent and mqtt_df("timestamp") <= failure_time)
//           .select("_id","timestamp", "_"+deviceName+"RemainingUsefulLife")
           .withColumn("_"+deviceName+"AboutToFail", lit("true"))

//        println("Binary lagging feature:")
//        binary_lagging_feature.orderBy(desc("timestamp")).show()

        // "RemainingUsefulLife" is a continuous lagging feature, calculated for all values since the last failure event indicated by "About To Fail" == true, and intended to be used to predict how much time is left before the next failure
        val continuous_lagging_feature = mqtt_df
            .filter(mqtt_df("timestamp") < failure_imminent and
              mqtt_df("_"+deviceName+"AboutToFail") === lit("false") and
              mqtt_df("_"+deviceName+"RemainingUsefulLife") === lit(0))
//            .select("_id","timestamp","_"+deviceName+"AboutToFail")
            .withColumn("_"+deviceName+"RemainingUsefulLife", lit(failure_imminent.toInt)-mqtt_df.col("timestamp"))

//        println("Continuous lagging feature:")
//        continuous_lagging_feature.orderBy(desc("timestamp")).show()

        // combine the two lagging features
//        val lag_vars = binary_lagging_feature.join(continuous_lagging_feature, Seq("_id","timestamp","_"+deviceName+"AboutToFail","_"+deviceName+"RemainingUsefulLife"), "outer")
        val lag_vars = continuous_lagging_feature.union(binary_lagging_feature)
//        lag_vars.orderBy(desc("timestamp")).show()
        // persist lagging features to MapR-DB
        lag_vars.write.option("Operation", "Update").saveToMapRDB(tableName)


        // print a summary of the records which have been updated
//        sc.loadFromMapRDB(tableName).where(field("timestamp") >= failure_imminent and field("timestamp") <= failure_time).select("timestamp","OutsideAirTemp","_"+deviceName+"AboutToFail")
        val mqtt_df2 = sc.loadFromMapRDB(tableName).toDF()
        println("Current MapR-DB table:")
        mqtt_df2
          .filter(mqtt_df2("timestamp") <= failure_time)
          .select("timestamp","_"+deviceName+"AboutToFail","_"+deviceName+"RemainingUsefulLife")
          .orderBy(desc("timestamp"))
          .show(rows_to_update.count.toInt+5)

        println("---done---")
      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}

