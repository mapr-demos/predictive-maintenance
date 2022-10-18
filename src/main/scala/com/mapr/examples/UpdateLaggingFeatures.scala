package com.mapr.examples

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.sql.SparkSession
import com.mapr.db.spark.sql._
import org.apache.spark.sql.functions._
import com.mapr.db.spark._
import com.mapr.db.spark.field

import java.io.DataOutputStream
import java.net.HttpURLConnection
import java.net.URL
import org.apache.commons.codec.binary.Base64

import java.security.cert.X509Certificate
import javax.net.ssl.{HostnameVerifier, HttpsURLConnection, SSLContext, TrustManager, X509TrustManager}
//import org.apache.spark.sql._

/******************************************************************************
  PURPOSE: Receive timestamps and device name from a stream, indicating when and where failures have occurred. Upon failure, this program will update all the lagging features corresponding to the failed device.

  BUILD:
    `mvn package`
    copy target/lib to your cluster

  SYNTHESIZE DATA:

    echo "{\"timestamp\":"$(date +%s)",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:failures --broker-list this.will.be.ignored:9092

  RUN:

 /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures target/factory-iot-tutorial-1.0-jar-with-dependencies.jar <stream:topic>[,<stream2:topic2>] <tableName> <Grafana URL>

  EXAMPLE:

  /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/factory:failures /apps/mqtt_records http://localhost:3000

 ****************************************************************************/

object UpdateLaggingFeatures {

  case class FailureEvent(timestamp: String,
                          deviceName: String) extends Serializable

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: UpdateLaggingFeatures <stream:topic> <table> <Grafana URL>")
      System.exit(1)
    }
    val grafana_url = args(2)
    println("Failures will be annotated on Grafana at " + grafana_url)
    val topicsSet = args(0).split(",").toSet
    println("Waiting for messages on stream " + args(0) + "...")

    //mykeystore.jks is the keystore
    //In Chrome click the lock icon to the left of the url. Then click "Certificate Information".Go to the
    //"Details" tab and click "Copy to file".Save it as a "base64 encoded X.509 (.cer)" to "SITENAME.cer".
    // Copy $JAVA_HOME/lib/security/cacerts to your application's directory as "mykeystore.jks".

    // Install the certificate
    //with: keytool -keystore mykeystore.jks -storepass changeit -importcert -alias SITENAME -trustcacerts -file SITE.cer
    //System.setProperty("javax.net.ssl.trustStore", "mykeystore.jks")

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
        println("**************************")
        rdd.collect().foreach(println)
        println("**************************")
        val rdd1 = rdd.filter( x => x !=null)
        rdd1.collect().foreach(println)
        println("**************************")
        val ds: Dataset[FailureEvent] = spark.read.schema(schema).json(rdd1).na.drop().as[FailureEvent]
        println("Failure events received from stream: " + args(0) + ":")
        val ds1 = ds.na.drop("any")
        ds1.show()


        // TODO: iterate on each reported failure, not just the first one.
        // iterate through all the timestamps like this:
        //        ds.select("timestamp").map(r => r.getString(0)).foreach(
        //          println(_)
        //        )

        // get the first failure time:
        val failure_time = ds1.select("timestamp").map(r => r.getString(0)).collect()(0).toString()
        // get the corresponding failed device:
        val deviceName = ds1.select("deviceName").map(r => r.getString(0)).collect()(0).toString()

        // Load the MQTT data table from MapR-DB JSON
        // The table schema will be inferred when we loadFromMapRDB:
        val mqtt_rdd = sc.loadFromMapRDB(tableName)
        val mqtt_df = mqtt_rdd.toDF()

        // We can select rows and filter on that RDD as shown below:
        println("Total number of records in table " + tableName + ": " + mqtt_rdd.map(a => {a.timestamp}).count)
        //        println(ds.filter(a => {a.timestamp >= failure_time}).map(a => {a.timestamp}).count)

        // Mark the device as "about to fail" for a period of time leading up to failure_time
        val failure_window = "30"  // in seconds, to match Unix time given by `date +%s`
        val failure_imminent = (failure_time.toInt - failure_window.toInt).toString

        // This try block catches org.apache.spark.sql.AnalysisException errors caused by trying to lookup columns for devices that aren't part of the schema
        try {
          // For more information about how to specify filter conditions with MapR-DB, see:
          // https://maprdocs.mapr.com/home/Spark/ScalaDSLforSpecifyingPredicates.html
          val rows_to_update = mqtt_rdd.where(field("timestamp") >= failure_imminent and field("timestamp") <= failure_time)

          // "AboutToFail" is a binary lagging feature intended to be used to classify whether failure is imminent
          val binary_lagging_feature = mqtt_df.filter(mqtt_df("timestamp") >= failure_imminent and mqtt_df("timestamp") <= failure_time)
            .withColumn("_" + deviceName + "AboutToFail", lit("true"))

          //        println("Binary lagging feature:")
          //        binary_lagging_feature.orderBy(desc("timestamp")).show()

          // "RemainingUsefulLife" is a continuous lagging feature, calculated for all values since the last failure event indicated by "About To Fail" == true, and intended to be used to predict how much time is left before the next failure
          val continuous_lagging_feature = mqtt_df
            .filter(mqtt_df("timestamp") < failure_imminent and
              mqtt_df("_" + deviceName + "AboutToFail") === lit("false") and
              mqtt_df("_" + deviceName + "RemainingUsefulLife") === lit(0))
            //            .select("_id","timestamp","_"+deviceName+"AboutToFail")
            .withColumn("_" + deviceName + "RemainingUsefulLife", lit(failure_imminent.toInt) - mqtt_df.col("timestamp"))

          println("_AboutToFail time window (" + failure_window + "s): t=" + failure_imminent + " to t=" + failure_time)
          println("_AboutToFail records updated: " + binary_lagging_feature.count)
          println("_RemainingUsefulLife records updated: " + continuous_lagging_feature.count)

          //        println("Continuous lagging feature:")
          //        continuous_lagging_feature.orderBy(desc("timestamp")).show()

          // combine the two lagging features
          //        val lag_vars = binary_lagging_feature.join(continuous_lagging_feature, Seq("_id","timestamp","_"+deviceName+"AboutToFail","_"+deviceName+"RemainingUsefulLife"), "outer")
          val lag_vars = continuous_lagging_feature.union(binary_lagging_feature)
          //        lag_vars.orderBy(desc("timestamp")).show()
          // persist lagging features to MapR-DB
          lag_vars.write.option("Operation", "InsertOrReplace").saveToMapRDB(tableName)


          // print a summary of the records which have been updated
          //        sc.loadFromMapRDB(tableName).where(field("timestamp") >= failure_imminent and field("timestamp") <= failure_time).select("timestamp","OutsideAirTemp","_"+deviceName+"AboutToFail")
          val mqtt_df2 = sc.loadFromMapRDB(tableName).toDF()
          println("Here is an excerpt of the lagging features updated in MapR-DB:")
          mqtt_df2
            .filter(mqtt_df2("timestamp") <= failure_time)
            .select("timestamp", "_" + deviceName + "AboutToFail", "_" + deviceName + "RemainingUsefulLife")
            .orderBy(desc("timestamp"))
            .show(rows_to_update.count.toInt + 5)

          // send notification to Grafana for visualization
          NotfyGrafana(grafana_url, "Failure event", deviceName + " failed")
        } catch {
          case e: org.apache.spark.sql.AnalysisException => println("Ignoring reported failure for unknown device " + deviceName)
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    println("---done---")
  }

  private val trustAllManager = {
    val manager = new X509TrustManager() {
      def getAcceptedIssuers: Array[X509Certificate] = null

      def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {}

      def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
    }
    Array[TrustManager](manager)
  }
  // HTTP POST request
  @throws[Exception]
  private def NotfyGrafana(grafana_url: String, title: String, text: String): Unit = {
    val sc = SSLContext.getInstance("SSL")
    sc.init(null, trustAllManager, new java.security.SecureRandom())
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory)

    import javax.net.ssl.SSLSession
    // Create all-trusting host name verifier// Create all-trusting host name verifier

    val allHostsValid = new HostnameVerifier() {
      def verify(hostname: String, session: SSLSession) = true
    }
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid)

    val USER_AGENT = "Mozilla/5.0"
    val url = grafana_url + "/api/annotations"
    val obj = new URL(url)
    val con = obj.openConnection.asInstanceOf[HttpURLConnection]
    val user_pass = "mapr:mapr"
    val encoded = Base64.encodeBase64String(user_pass.getBytes)
    con.setRequestProperty("Authorization", "Basic " + encoded)
    con.setRequestMethod("POST")
    con.setRequestProperty("User-Agent", USER_AGENT)
    con.setRequestProperty("Accept", "*/*")
    val unixTime = System.currentTimeMillis
    val urlParameters = "&time=" + unixTime + "&title=" + title + "&text=" + text + "&tags=UpdateLaggingFeatures"
    // Send post request
    con.setDoOutput(true)
    val wr = new DataOutputStream(con.getOutputStream)
    wr.writeBytes(urlParameters)
    wr.flush()
    wr.close()
    val responseCode = con.getResponseCode
    println("\nSending fault notification to Grafana: " + url)
    println("Response Code: " + responseCode)
  }

}