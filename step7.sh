#!/usr/bin/env bash
java -cp ~/factory-iot-tutorial/target/factory-iot-tutorial-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10
