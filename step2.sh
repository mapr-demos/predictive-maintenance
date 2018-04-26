#!/usr/bin/env bash
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.MqttConsumer ~/factory-iot-tutorial/target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/factory:mqtt /apps/mqtt_records
