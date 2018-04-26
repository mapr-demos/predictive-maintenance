#!/usr/bin/env bash
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform ~/factory-iot-tutorial/target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 25.0 http://localhost:3000
