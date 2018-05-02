#!/usr/bin/env bash
# Step 7 - Synthesize a high speed data stream:
java -cp ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10
