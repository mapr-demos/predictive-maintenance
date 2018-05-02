#!/usr/bin/env bash
# Step 2 - Save IoT data stream to MapR-DB:
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.MqttConsumer ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/factory:mqtt /apps/mqtt_records
