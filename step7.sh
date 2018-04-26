#!/usr/bin/env bash
java -cp ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10
