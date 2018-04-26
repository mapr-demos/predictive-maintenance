#!/usr/bin/env bash
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/factory:failures /apps/mqtt_records http://localhost:3000
