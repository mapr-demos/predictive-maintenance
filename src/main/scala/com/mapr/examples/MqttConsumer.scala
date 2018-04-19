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

/******************************************************************************
  PURPOSE:

  Save all metrics from an MQTT dataset to a MapR-DB table.

  BUILD:

  `mvn package`
  copy target/lib to your cluster

  SYNTHESIZE DATA:

  cat sample_dataset/mqtt.json | while read line; do echo $line | sed 's/{/{"timestamp":"'$(date +%s)'",/g' | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:opto22 --broker-list this.will.be.ignored:9092; echo -n "."; done

  RUN:

  /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.MqttConsumer target/factory-iot-tutorial-1.0-jar-with-dependencies.jar <stream:topic>[,<stream2:topic2>] <tableName>

  EXAMPLE:

  /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.MqttConsumer target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/mqtt:opto22 /apps/mqtt_records

  ****************************************************************************/

object MqttConsumer {

  case class MqttRecord(timestamp: String,
                        OutsideAirTemp: String,
                        DaylightSensor: String,
                        BoilerReturnTemp: String,
                        BoilerFlowTemp: String,
                        CentralPlantOutsideAirTemp: String,
                        CentralPlantRelativeHumidity: String,
                        AHU1SupplyAirTemp: String,
                        AHU1ReturnAirTemp: String,
                        AHU1ColdWaterValve: String,
                        AHU1HotWaterValve: String,
                        AHU2SupplyAirTemp: String,
                        AHU2ReturnAirTemp: String,
                        AHU2ColdWaterValve: String,
                        AHU2HotWaterValve: String,
                        AHU3SupplyAirTemp: String,
                        AHU3ReturnAirTemp: String,
                        AHU3ColdWaterValve: String,
                        AHU3HotWaterValve: String,
                        AHU4SupplyAirTemp: String,
                        AHU4ReturnAirTemp: String,
                        AHU4ColdWaterValve: String,
                        AHU4HotWaterValve: String,
                        Chiller1FlowTemp: String,
                        Chiller1ReturnTemp: String,
                        Chiller2FlowTemp: String,
                        Chiller2ReturnTemp: String,
                        ChilledWaterBypassValve: String,
                        Chiller1PumpStatus: String,
                        Chiller2PumpStatus: String,
                        Boiler1PumpStatus: String,
                        Boiler2PumpStatus: String,
                        AHU1ReturnAirFanStatus: String,
                        AHU1SupplyAirFanStatus: String,
                        AHU2ReturnAirFanStatus: String,
                        AHU2SupplyAirFanStatus: String,
                        AHU3ReturnAirFanStatus: String,
                        AHU3SupplyAirFanStatus: String,
                        AHU4ReturnAirFanStatus: String,
                        AHU4SupplyAirFanStatus: String,
                        BuildingPower: String,
                        Panel1Power: String,
                        Panel2Power: String,
                        Panel3Power: String,
                        FC1Temp: String,
                        FC1ColdWaterValve: String,
                        FC1HotWaterValve: String,
                        FC2Temp: String,
                        FC2ColdWaterValve: String,
                        FC2HotWaterValve: String,
                        FC3Temp: String,
                        FC3ColdWaterValve: String,
                        FC3HotWaterValve: String,
                        FC4Temp: String,
                        FC4ColdWaterValve: String,
                        FC4HotWaterValve: String,
                        FC5Temp: String,
                        FC5ColdWaterValve: String,
                        FC5HotWaterValve: String,
                        FC6Temp: String,
                        FC6ColdWaterValve: String,
                        FC6HotWaterValve: String,
                        FC7Temp: String,
                        FC7ColdWaterValve: String,
                        FC7HotWaterValve: String,
                        FC7Setpoint: String,
                        FC8Temp: String,
                        FC8ColdWaterValve: String,
                        FC8HotWaterValve: String,
                        FC8Setpoint: String,
                        FC9Temp: String,
                        FC9ColdWaterValve: String,
                        FC9HotWaterValve: String,
                        FC9Setpoint: String,
                        FC10Temp: String,
                        FC10ColdWaterValve: String,
                        FC10HotWaterValve: String,
                        FC10Setpoint: String,
                        FC11Temp: String,
                        FC11ColdWaterValve: String,
                        FC11HotWaterValve: String,
                        FC11Setpoint: String,
                        FC12Temp: String,
                        FC12ColdWaterValve: String,
                        FC12HotWaterValve: String,
                        FC12Setpoint: String,
                        FC13Temp: String,
                        FC13ColdWaterValve: String,
                        FC13HotWaterValve: String,
                        FC13Setpoint: String,
                        FC14Temp: String,
                        FC14ColdWaterValve: String,
                        FC14HotWaterValve: String,
                        FC14Setpoint: String,
                        FC15Temp: String,
                        FC15ColdWaterValve: String,
                        FC15HotWaterValve: String,
                        FC16Temp: String,
                        FC16ColdWaterValve: String,
                        FC16HotWaterValve: String,
                        FC16Setpoint: String,
                        FC17Temp: String,
                        FC17ColdWaterValve: String,
                        FC17HotWaterValve: String,
                        FC17Setpoint: String,
                        FC18Temp: String,
                        FC18ColdWaterValve: String,
                        FC18HotWaterValve: String,
                        FC18Setpoint: String,
                        FC19Temp: String,
                        FC19ColdWaterValve: String,
                        FC19HotWaterValve: String,
                        FC19Setpoint: String,
                        FC20Temp: String,
                        FC20ColdWaterValve: String,
                        FC20HotWaterValve: String,
                        FC20Setpoint: String,
                        FC21Temp: String,
                        FC21ColdWaterValve: String,
                        FC21HotWaterValve: String,
                        FC21Setpoint: String,
                        FC22Temp: String,
                        FC22ColdWaterValve: String,
                        FC22HotWaterValve: String,
                        FC22Setpoint: String,
                        FC23Temp: String,
                        FC23ColdWaterValve: String,
                        FC23HotWaterValve: String,
                        FC23Setpoint: String,
                        FC24Temp: String,
                        FC24ColdWaterValve: String,
                        FC24HotWaterValve: String,
                        FC24Setpoint: String,
                        FC26Temp: String,
                        FC26ColdWaterValve: String,
                        FC26HotWaterValve: String,
                        FC26Setpoint: String,
                        Chiller1Start: String,
                        Chiller2Start: String,
                        BoilerStart: String,
                        AH1OaInDamper: String,
                        AH1ReturnDamper: String,
                        AH1OaOutDamper: String,
                        AH2OaInDamper: String,
                        AH2ReturnDamper: String,
                        AH2OaOutDamper: String,
                        AH3OaInDamper: String,
                        AH3ReturnDamper: String,
                        AH3OaOutDamper: String,
                        AH4OaInDamper: String,
                        AH4ReturnDamper: String,
                        AH4OaOutDamper: String) extends Serializable

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: MqttConsumer <stream:topic> <table>")
      System.exit(1)
    }
    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = true),
      StructField("OutsideAirTemp", StringType, nullable = true),
      StructField("DaylightSensor", StringType, nullable = true),
      StructField("BoilerReturnTemp", StringType, nullable = true),
      StructField("BoilerFlowTemp", StringType, nullable = true),
      StructField("CentralPlantOutsideAirTemp", StringType, nullable = true),
      StructField("CentralPlantRelativeHumidity", StringType, nullable = true),
      StructField("AHU1SupplyAirTemp", StringType, nullable = true),
      StructField("AHU1ReturnAirTemp", StringType, nullable = true),
      StructField("AHU1ColdWaterValve", StringType, nullable = true),
      StructField("AHU1HotWaterValve", StringType, nullable = true),
      StructField("AHU2SupplyAirTemp", StringType, nullable = true),
      StructField("AHU2ReturnAirTemp", StringType, nullable = true),
      StructField("AHU2ColdWaterValve", StringType, nullable = true),
      StructField("AHU2HotWaterValve", StringType, nullable = true),
      StructField("AHU3SupplyAirTemp", StringType, nullable = true),
      StructField("AHU3ReturnAirTemp", StringType, nullable = true),
      StructField("AHU3ColdWaterValve", StringType, nullable = true),
      StructField("AHU3HotWaterValve", StringType, nullable = true),
      StructField("AHU4SupplyAirTemp", StringType, nullable = true),
      StructField("AHU4ReturnAirTemp", StringType, nullable = true),
      StructField("AHU4ColdWaterValve", StringType, nullable = true),
      StructField("AHU4HotWaterValve", StringType, nullable = true),
      StructField("Chiller1FlowTemp", StringType, nullable = true),
      StructField("Chiller1ReturnTemp", StringType, nullable = true),
      StructField("Chiller2FlowTemp", StringType, nullable = true),
      StructField("Chiller2ReturnTemp", StringType, nullable = true),
      StructField("ChilledWaterBypassValve", StringType, nullable = true),
      StructField("Chiller1PumpStatus", StringType, nullable = true),
      StructField("Chiller2PumpStatus", StringType, nullable = true),
      StructField("Boiler1PumpStatus", StringType, nullable = true),
      StructField("Boiler2PumpStatus", StringType, nullable = true),
      StructField("AHU1ReturnAirFanStatus", StringType, nullable = true),
      StructField("AHU1SupplyAirFanStatus", StringType, nullable = true),
      StructField("AHU2ReturnAirFanStatus", StringType, nullable = true),
      StructField("AHU2SupplyAirFanStatus", StringType, nullable = true),
      StructField("AHU3ReturnAirFanStatus", StringType, nullable = true),
      StructField("AHU3SupplyAirFanStatus", StringType, nullable = true),
      StructField("AHU4ReturnAirFanStatus", StringType, nullable = true),
      StructField("AHU4SupplyAirFanStatus", StringType, nullable = true),
      StructField("BuildingPower", StringType, nullable = true),
      StructField("Panel1Power", StringType, nullable = true),
      StructField("Panel2Power", StringType, nullable = true),
      StructField("Panel3Power", StringType, nullable = true),
      StructField("FC1Temp", StringType, nullable = true),
      StructField("FC1ColdWaterValve", StringType, nullable = true),
      StructField("FC1HotWaterValve", StringType, nullable = true),
      StructField("FC2Temp", StringType, nullable = true),
      StructField("FC2ColdWaterValve", StringType, nullable = true),
      StructField("FC2HotWaterValve", StringType, nullable = true),
      StructField("FC3Temp", StringType, nullable = true),
      StructField("FC3ColdWaterValve", StringType, nullable = true),
      StructField("FC3HotWaterValve", StringType, nullable = true),
      StructField("FC4Temp", StringType, nullable = true),
      StructField("FC4ColdWaterValve", StringType, nullable = true),
      StructField("FC4HotWaterValve", StringType, nullable = true),
      StructField("FC5Temp", StringType, nullable = true),
      StructField("FC5ColdWaterValve", StringType, nullable = true),
      StructField("FC5HotWaterValve", StringType, nullable = true),
      StructField("FC6Temp", StringType, nullable = true),
      StructField("FC6ColdWaterValve", StringType, nullable = true),
      StructField("FC6HotWaterValve", StringType, nullable = true),
      StructField("FC7Temp", StringType, nullable = true),
      StructField("FC7ColdWaterValve", StringType, nullable = true),
      StructField("FC7HotWaterValve", StringType, nullable = true),
      StructField("FC7Setpoint", StringType, nullable = true),
      StructField("FC8Temp", StringType, nullable = true),
      StructField("FC8ColdWaterValve", StringType, nullable = true),
      StructField("FC8HotWaterValve", StringType, nullable = true),
      StructField("FC8Setpoint", StringType, nullable = true),
      StructField("FC9Temp", StringType, nullable = true),
      StructField("FC9ColdWaterValve", StringType, nullable = true),
      StructField("FC9HotWaterValve", StringType, nullable = true),
      StructField("FC9Setpoint", StringType, nullable = true),
      StructField("FC10Temp", StringType, nullable = true),
      StructField("FC10ColdWaterValve", StringType, nullable = true),
      StructField("FC10HotWaterValve", StringType, nullable = true),
      StructField("FC10Setpoint", StringType, nullable = true),
      StructField("FC11Temp", StringType, nullable = true),
      StructField("FC11ColdWaterValve", StringType, nullable = true),
      StructField("FC11HotWaterValve", StringType, nullable = true),
      StructField("FC11Setpoint", StringType, nullable = true),
      StructField("FC12Temp", StringType, nullable = true),
      StructField("FC12ColdWaterValve", StringType, nullable = true),
      StructField("FC12HotWaterValve", StringType, nullable = true),
      StructField("FC12Setpoint", StringType, nullable = true),
      StructField("FC13Temp", StringType, nullable = true),
      StructField("FC13ColdWaterValve", StringType, nullable = true),
      StructField("FC13HotWaterValve", StringType, nullable = true),
      StructField("FC13Setpoint", StringType, nullable = true),
      StructField("FC14Temp", StringType, nullable = true),
      StructField("FC14ColdWaterValve", StringType, nullable = true),
      StructField("FC14HotWaterValve", StringType, nullable = true),
      StructField("FC14Setpoint", StringType, nullable = true),
      StructField("FC15Temp", StringType, nullable = true),
      StructField("FC15ColdWaterValve", StringType, nullable = true),
      StructField("FC15HotWaterValve", StringType, nullable = true),
      StructField("FC16Temp", StringType, nullable = true),
      StructField("FC16ColdWaterValve", StringType, nullable = true),
      StructField("FC16HotWaterValve", StringType, nullable = true),
      StructField("FC16Setpoint", StringType, nullable = true),
      StructField("FC17Temp", StringType, nullable = true),
      StructField("FC17ColdWaterValve", StringType, nullable = true),
      StructField("FC17HotWaterValve", StringType, nullable = true),
      StructField("FC17Setpoint", StringType, nullable = true),
      StructField("FC18Temp", StringType, nullable = true),
      StructField("FC18ColdWaterValve", StringType, nullable = true),
      StructField("FC18HotWaterValve", StringType, nullable = true),
      StructField("FC18Setpoint", StringType, nullable = true),
      StructField("FC19Temp", StringType, nullable = true),
      StructField("FC19ColdWaterValve", StringType, nullable = true),
      StructField("FC19HotWaterValve", StringType, nullable = true),
      StructField("FC19Setpoint", StringType, nullable = true),
      StructField("FC20Temp", StringType, nullable = true),
      StructField("FC20ColdWaterValve", StringType, nullable = true),
      StructField("FC20HotWaterValve", StringType, nullable = true),
      StructField("FC20Setpoint", StringType, nullable = true),
      StructField("FC21Temp", StringType, nullable = true),
      StructField("FC21ColdWaterValve", StringType, nullable = true),
      StructField("FC21HotWaterValve", StringType, nullable = true),
      StructField("FC21Setpoint", StringType, nullable = true),
      StructField("FC22Temp", StringType, nullable = true),
      StructField("FC22ColdWaterValve", StringType, nullable = true),
      StructField("FC22HotWaterValve", StringType, nullable = true),
      StructField("FC22Setpoint", StringType, nullable = true),
      StructField("FC23Temp", StringType, nullable = true),
      StructField("FC23ColdWaterValve", StringType, nullable = true),
      StructField("FC23HotWaterValve", StringType, nullable = true),
      StructField("FC23Setpoint", StringType, nullable = true),
      StructField("FC24Temp", StringType, nullable = true),
      StructField("FC24ColdWaterValve", StringType, nullable = true),
      StructField("FC24HotWaterValve", StringType, nullable = true),
      StructField("FC24Setpoint", StringType, nullable = true),
      StructField("FC26Temp", StringType, nullable = true),
      StructField("FC26ColdWaterValve", StringType, nullable = true),
      StructField("FC26HotWaterValve", StringType, nullable = true),
      StructField("FC26Setpoint", StringType, nullable = true),
      StructField("Chiller1Start", StringType, nullable = true),
      StructField("Chiller2Start", StringType, nullable = true),
      StructField("BoilerStart", StringType, nullable = true),
      StructField("AH1OaInDamper", StringType, nullable = true),
      StructField("AH1ReturnDamper", StringType, nullable = true),
      StructField("AH1OaOutDamper", StringType, nullable = true),
      StructField("AH2OaInDamper", StringType, nullable = true),
      StructField("AH2ReturnDamper", StringType, nullable = true),
      StructField("AH2OaOutDamper", StringType, nullable = true),
      StructField("AH3OaInDamper", StringType, nullable = true),
      StructField("AH3ReturnDamper", StringType, nullable = true),
      StructField("AH3OaOutDamper", StringType, nullable = true),
      StructField("AH4OaInDamper", StringType, nullable = true),
      StructField("AH4ReturnDamper", StringType, nullable = true),
      StructField("AH4OaOutDamper", StringType, nullable = true)
    ))

    val groupId = "testgroup"
    val offsetReset = "earliest"  //  "latest"
    val pollTimeout = "5000"
    val brokers = "this.will.be.ignored:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(MqttConsumer.getClass.getName).setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

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
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
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
        println("Messages consumed from stream "+args(0)+": " + count)
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val ds: Dataset[MqttRecord] = spark.read.schema(schema).json(rdd).as[MqttRecord]
//        ds.show
//        ds.take(1).foreach(println(_))
        // Apply higher-level Dataset API methods such as groupBy() and avg().
        // Filter temperatures > 25, along with their corresponding
        // devices' humidity, compute averages, groupBy cca3 country codes,
        // and display the results, using table and bar charts
//        val dsAvgTmp = ds.filter(d => {d.Panel2Power != "0"}).map(d => (d.Chiller1PumpStatus, d.Chiller2PumpStatus, d.Boiler1PumpStatus, d.Boiler2PumpStatus, d.Panel1Power, d.Panel2Power, d.Panel3Power))

        // Here's an example showing how to filter a dataset
//        println(ds.filter(d => {d.Chiller1PumpStatus == "1"}).count()+"\n"+
//        ds.filter(d => {d.Chiller2PumpStatus == "1"}).count()+"\n"+
//        ds.filter(d => {d.Boiler1PumpStatus == "1"}).count()+"\n"+
//        ds.filter(d => {d.Boiler2PumpStatus == "1"}).count()+"\n")

        // Here's an example showing how to only look at data while the factory is operating (because Panel2Power will always be nonzero then)
//        val ds2 = ds.filter(d => {d.Panel2Power != "0"}).map(d => (d.Chiller1PumpStatus, d.Chiller2PumpStatus, d.Boiler1PumpStatus, d.Boiler2PumpStatus, d.Panel1Power, d.Panel2Power, d.Panel3Power))
        ds.select("timestamp","OutsideAirTemp","Panel1Power","Panel2Power","Panel3Power").show
        // Here's another way to show those columns:
        // ds.map(d => (d.timestamp, d.OutsideAirTemp, d.Panel1Power, d.Panel2Power, d.Panel3Power)).show()


        // Every time we receive a new metric data, we want to derive a new metric called "about to fail". This is a lagging feature which we'll retroactively update once a failure occurs so that we can use that data for training a predictive maintenance model. So, here's how to add that metric to what we're saving in MapR-DB:
        // Use underscore in the column name to denote that this column was derived
        // create a column which can be used as an index:
        // create other columns to be used as lagging features for supervised ML
        val ds3 = ds
          .withColumn("_Chiller1AboutToFail", lit("false"))
          .withColumn("_Chiller1RemainingUsefulLife", lit("0"))
          .withColumn("_Chiller2AboutToFail", lit("false"))
          .withColumn("_Chiller2RemainingUsefulLife", lit("0"))
          .withColumn("_Boiler1AboutToFail", lit("false"))
          .withColumn("_Boiler1RemainingUsefulLife", lit("0"))
          .withColumn("_Boiler2AboutToFail", lit("false"))
          .withColumn("_Boiler2RemainingUsefulLife", lit("0"))
//          .select($"*", unix_timestamp($"timestamp", "s").as("unix_time"))
//          .selectExpr("*", "to_date(unix_time) as _date_value")
//          .withColumn("_date_key",expr("date_format(_date_value, 'YYYYMMdd')"))
          .withColumn("_year_key", year(from_unixtime(col("timestamp"))))
          .withColumn("_quarter_of_year", quarter(from_unixtime(col("timestamp"))))
          .selectExpr("*", s"concat('Q', _quarter_of_year) as _quarter_short")
          .withColumn("_month_of_year", month(from_unixtime(col("timestamp"))))
          .withColumn("_day_number_of_week", from_unixtime(col("timestamp"), "u").cast("Int"))
          .selectExpr("*", """CASE WHEN _day_number_of_week > 5 THEN true ELSE false END as _weekend""")
          .withColumn("_day_of_week_long", from_unixtime(col("timestamp"), "EEEEEEEEE"))
          .withColumn("_month_long", from_unixtime(col("timestamp"), "MMMMMMMM"))
//          .withColumn("_week_key",expr("date_format(_date_value, 'ww')"))

        // Select only fields that are primitive string or number types for saving to MapR-DB.
        // Note, you can't persist the TimestampType field because it can't be converted an OJAI type
//        val ds5 = ds3.join(ds4.select("timestamp","_year_key","_week_key","_month_of_year","_month_long","_day_number_of_week","_day_of_week_long","_weekend","_quarter_of_year","_quarter_short"), Seq("timestamp"))

        println("Saving " + ds3.count() + " new records to table " + args(1))
        try{
          ds3.saveToMapRDB(tableName = args(1), idFieldPath = "timestamp", createTable = false)
        } catch {
          case e: com.mapr.db.exceptions.TableNotFoundException =>
            ds3.saveToMapRDB(tableName = args(1), idFieldPath = "timestamp", createTable = true)
        }

        //        df.createOrReplaceTempView("mqtt_snapshot")
        val mqtt_rdd = spark.loadFromMapRDB(args(1))
        println("Number of rows in table " + args(1) + ": " + mqtt_rdd.count)

//        spark.sql("select count(*) from " +  args(1)).show
      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
