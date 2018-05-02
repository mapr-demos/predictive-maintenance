#!/usr/bin/env bash
# Step 8 - Process high speed data stream:
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 20.0 http://localhost:3000
