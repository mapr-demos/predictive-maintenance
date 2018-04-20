package com.mapr.examples

import com.mapr.db.spark.{field, _}
import com.mapr.db.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import breeze.linalg.{DenseVector, norm}
import breeze.signal._
import java.io.DataOutputStream
import java.net.HttpURLConnection
import java.net.URL
import org.apache.commons.codec.binary.Base64


/******************************************************************************
  PURPOSE:

  Calculate Fourier transforms for streaming time-series data. This is intended to demonstrate how to detect anomalies in data from vibration sensors.

  BUILD:

  mvn package
  copy target/lib to your cluster

  PRELIMINARY:

  Simulate a stream of fast vibration data with this command:

  java -cp target/factory-iot-tutorial-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10 http://localhost:3000

  RUN:

  /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform target/factory-iot-tutorial-1.0-jar-with-dependencies.jar <stream:topic> <vibration_change_threshold> <Grafana URL>

  EXAMPLE:

  /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 25.0 http://localhost:3000

  ****************************************************************************/

object StreamingFourierTransform {

  case class Signal(t: Double, amplitude: Double) extends Serializable

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("USAGE: StreamingFourierTransform <stream:topic> [deviation_threshold]")
      System.err.println("EXAMPLE: spark-submit --class com.mapr.examples.StreamingFourierTransform target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/fastdata:vibration 25.0 http://localhost:3000")
      System.exit(1)
    }
    val deviation_tolerance = args(1).toDouble
    println("Alerting when FFT similarity changes more than " + deviation_tolerance + "%")
    val grafana_url = args(2)
    println("Failures will be annotated on Grafana at " + grafana_url)
    println("Waiting for messages on stream " + args(0) + "...")

    val schema = StructType(Array(
      StructField("t", DoubleType, nullable = true),
      StructField("amplitude", DoubleType, nullable = true)
    ))
    val groupId = "testgroup"
    val offsetReset = "latest"  //  "earliest"
    val pollTimeout = "5000"
    val brokers = "this.will.be.ignored:9092" // not needed for MapR Streams, needed for Kafka
    val sparkConf = new SparkConf()
      .setAppName(StreamingFourierTransform.getClass.getName).setMaster("local[*]")
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
    // Initialize FFT comparison
    var b = breeze.linalg.DenseVector[Double]()
    var msg_counter: Long = 0
    var t0 = System.nanoTime()
    valuesDStream.foreachRDD { (rdd: RDD[String]) =>
      if (!rdd.isEmpty) {
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val ds: Dataset[Signal] = spark.read.schema(schema).json(rdd).as[Signal]
        val throughput: Double = ds.count().toDouble / ((System.nanoTime() - t0) / 1000000000d)
        msg_counter += ds.count()
        t0 = System.nanoTime()
        val required_sample_size = 600
        // Consequtive RDDs may be out of phase, so just take the last 600 and sort them
        // so we're always taking FFTs on consistent representations of the time-domain signal
        val amplitude_series = ds
          .limit(required_sample_size)
          .sort(asc("t"))
          .select("amplitude")
          .collect.map(_.getDouble(0)).take(required_sample_size)
        // Skip FFT calculation if we have insufficient samples
        if (amplitude_series.length < required_sample_size) {
          println("Insufficient samples to determine frequency profile")
        } else {
          // Convert amplitude series to frequency domain using Fourier Transform
          val fft = fourierTr(DenseVector(amplitude_series: _*))
          // Calculate the similarity between this fft and the last one we calculated.
          // Drop the phase from the FFT so we're just dealing with real numbers.
          val a = fft.map(x => x.real)
          // Skip similarity calculation for the first RDD, and when RDDs vary drastically in size.
          if (b.length > 0 && (a.length/b.length < 2)) {
            // Cosine similarity formula: (a dot b) / math.sqrt((a dot a) * (b dot b))
            val cosine_similarity = math.abs((a dot b) / math.sqrt((a dot a) * (b dot b)))
            val fft_change = 100d-cosine_similarity*100d
            //            println(f"FFT similarity: $cosine_similarity%2.2f")
            println(f"Consumer throughput = $throughput%2.0f msgs/sec. Message count = $msg_counter. Rolling FFT similarity = $fft_change%2.2f%%")
            if (fft_change > deviation_tolerance) {
              println("<---------- SIMULATING FAILURE EVENT ---------->")
              // send notification to grafana for visualization
              NotfyGrafana(grafana_url, "Anomoly Detected", "Vibration fluctuated by " + fft_change + "%")
            }

          }
          // Save this FFT so we can measure rate of change for the next RDD.
          b = a
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  // HTTP POST request
  @throws[Exception]
  private def NotfyGrafana(grafana_url: String, title: String, text: String): Unit = {
    val USER_AGENT = "Mozilla/5.0"
    val url = grafana_url + "/api/annotations"
    val obj = new URL(url)
    val con = obj.openConnection.asInstanceOf[HttpURLConnection]
    val user_pass = "admin:admin"
    val encoded = Base64.encodeBase64String(user_pass.getBytes)
    con.setRequestProperty("Authorization", "Basic " + encoded)
    con.setRequestMethod("POST")
    con.setRequestProperty("User-Agent", USER_AGENT)
    con.setRequestProperty("Accept", "*/*")
    val unixTime = System.currentTimeMillis
    val urlParameters = "&time=" + unixTime + "&title=" + title + "&text=" + text + "&tag=HighSpeedProducer"
    // Send post request
    con.setDoOutput(true)
    val wr = new DataOutputStream(con.getOutputStream)
    wr.writeBytes(urlParameters)
    wr.flush()
    wr.close()
    val responseCode = con.getResponseCode
    System.out.println("\nSending fault notification to Grafana: " + url)
    System.out.println("Response Code: " + responseCode)
  }

}

