#!/usr/bin/env bash
###############################################################################
# This script runs all the programs in the predictive maintenance tutorial.
# Each program is run in a screen terminal and backgrounded.
###############################################################################

cd ~/factory-iot-tutorial

screen -dm -S step1-iot_producer bash -c "./step1.sh"
sleep 1
screen -dm -S step2-save2maprdb bash -c "./step2.sh"
sleep 1
screen -dm -S step3-save2tsdb bash -c "./step3.sh"
sleep 1
screen -dm -S step4-lagging_update bash -c "./step4.sh"
sleep 1
screen -dm -S step5-device_failure bash -c "./step5.sh"
sleep 1
screen -dm -S step7-vibration_producer bash -c "./step7.sh"
sleep 1
screen -dm -S step8-anomaly_detector bash -c "./step8.sh"
sleep 1
screen -list