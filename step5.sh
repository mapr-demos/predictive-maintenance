#!/usr/bin/env bash
# Step 5 - Simulate a failure event:
echo "{\"timestamp\":"$(date +%s -d '60 sec ago')",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --topic /apps/factory:failures --broker-list this.will.be.ignored:9092
