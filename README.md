
# MapR for Predictive Maintenance

This project is intended to show how to build Predictive Maintenance applications on MapR. Predictive Maintenance applications place high demands on data streaming, time-series data storage, and machine learning. Therefore, this project focuses on data ingest with MapR Streams, time-series data storage with MapR-DB and OpenTSDB, and feature engineering with MapR-DB and Apache Spark.

# Overview:

Predictive Maintenance requires a cutting edge data platform in order to handle fast streams of  IoT data, with the processing required for on-the-fly feature engineering, and the flexibility required for data science and machine learning.

Predictive Maintenance applications rely heavily on ingesting multiple data sources, each with their own format and throughput. MapR Streams can ingest data, regardless of format or speed,  with standard Kafka and RESTful APIs.

The "predictive" aspects of Predictive Maintenance applications are usually realized through machine learning. However, feature engineering is the most important aspect of machine learning, and it places the high demands on the data layer because the amount of data that IoT data streams generate and the tendency for failures to occur infrequently and without warning means vast amounts of raw time-series data must be stored. Not only must it be stored, but it must also be possible to retroactively update the lagging features necessary in order to label failures for the purposes of supervised machine learning. MapR-DB and Spark can work together to provide the capabiltieis required to put machine learning into practice for predictive maintance.

In summary:

* MapR Streams provide a convenient way to ingest IoT data because it is scalable and provides convenient interfaces.
* The integration of MapR DB with Spark provides a convenient way to label lagging features needed for predicting failures via supervised Machine Learning.
* Drill provides a convenient way to load ML data sets into Tensorflow for unsupervised and supervised machine learning

# Implementation Summary

There are two objectives relating to predictive maintenance implemented in this project. The first objective is to visualize time-series data in an interactive real-time dashboard in Grafana. The second objective is to make raw data streams and derived features available to machine learning frameworks, such as Tensorflow, in order to develop algorithms for anomaly detection and predictive maintenance. These two objects are realized using two seperate data flows:

1. The first flow, located on the top half of the image below, is intended to persist IoT data and label training data for sequence prediction and anomaly detection of time-series data in Tensorflow. 
2. The second flow, located on the bottom half, is intended to persist time-series IoT data in OpenTSDB for visualization in a Grafana dashboard. 

![data flow diagram](/images/dataflow.png?raw=true "Data Flow")

# Preliminary Steps

## Install OpenTSDB and Grafana:

Download and install `mapr-grafana`, `mapr-asynchbase`, and `mapr-opentsdb` from []http://artifactory.devops.lab/artifactory/prestage/releases-dev/MEP/](]http://artifactory.devops.lab/artifactory/prestage/releases-dev/MEP/)

```
yum install mapr-opentsdb -y
yum install mapr-grafana -y
```

Enabled write access to opentsdb:

```
cat /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf | sed "s/#tsd.mode = ro/tsd.mode = rw/g" > /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf.new
mv /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf.new /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf
chown mapr:mapr /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf
```

Start OpenTSDB and Grafana:

```
/opt/mapr/server/configure.sh -R -OT `hostname -f`
/opt/mapr/opentsdb/opentsdb-2.4.0/etc/init.d/opentsdb start
```

Open Grafana data sources, with a URL like [http://maprdemo:3000/datasources/edit/1](http://maprdemo:3000/datasources/edit/1), and add OpenTSDB as a new data source.

## Download tutorial code:

`git clone https://github.com/mapr-demos/factory-iot-tutorial`

## Compile:

```
mvn compile
mvn package
mvn install
```

## Create streams:

```
maprcli stream delete -path /apps/mqtt 
maprcli stream create -path /apps/mqtt -produceperm p -consumeperm p -topicperm p -ttl 900 -json
maprcli stream topic create -path /apps/mqtt -topic opto22 -partitions 1 -json
maprcli stream topic create -path /apps/mqtt -topic failures -partitions 1 -json
maprcli stream create -path /apps/fastdata -produceperm p -consumeperm p -topicperm p -ttl 900 -json
maprcli stream topic create -path /apps/fastdata -topic vibrations -partitions 1 -json
```

# Predictive Maintenance Demo Procedure

## STEP 1 - Synthesize MQTT stream:

This will stream 150 metrics roughly once a second.

```
cd sample_dataset
gunzip mqtt.json.gz
cat mqtt.json | while read line; do echo $line | sed 's/{/{"timestamp":"'$(date +%s)'",/g' | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:opto22 --broker-list this.will.be.ignored:9092; echo -n "."; sleep 1; done
```

## STEP 2 - Save MQTT stream to MapR-DB:

```
/opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.MqttConsumer target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/mqtt:opto22 /apps/mqtt_records
```

## STEP 3 - Save MQTT stream to OpenTSDB:

This process sends the MQTT data stream to OpenTSDB, where it will be loaded by Grafana in a dashboard. 

Update `localhost` with the hostname of the node running OpenTSDB.

```
/opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-consumer.sh --topic /apps/mqtt:opto22 --new-consumer --bootstrap-server not.applicable:0000 | while read line; do echo $line | jq -r "to_entries | map(\"\(.key) \(.value | tostring)\") | {t: .[0], x: .[]} | .[]" | paste -d ' ' - - | awk '{system("curl -X POST --data \x27{\"metric\": \""$3"\", \"timestamp\": "$2", \"value\": "$4", \"tags\": {\"host\": \"localhost\"}}\x27 http://localhost:4242/api/put")}'; echo -n "."; done
```

## STEP 4 - Update lagging features in MapR-DB for each failure event:

This process will listen for failure events on a MapR Streams topic and retroactively label lagging features in MapR-DB when failures occur, as well as render the failure event in Grafana. Update "http://localhost:3000" with the hostname and port for your Grafana instance.

```
/opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/mqtt:failures /apps/mqtt_records http://localhost:3000
```

## STEP 5 - Simulate a failure event:

```
echo "{\"timestamp\":"$(date +%s -d '60 sec ago')",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:failures --broker-list this.will.be.ignored:9092
```

## STEP 6 - Validate that lagging features have been updated:

```
$ mapr dbshell
find /apps/mqtt_records --where '{ "$eq" : {"_Chiller1AboutToFail":true} }' --f _id,_Chiller1AboutToFail,timestamp
find /apps/mqtt_records --where '{ "$eq" : {"timestamp":"1523339687"} }' --f _id,_Chiller1AboutToFail,timestamp
```

Here are a few examples commands to look at that table with `mapr dbshell`:

```
$ mapr dbshell
find /apps/mqtt_records --f timestamp
find --table /apps/mqtt_records --orderby _id --fields _Chiller2RemainingUsefulLife,_id
find --table /apps/mqtt_records --where '{"$gt" : {"_id" : "1523079964"}}' --orderby _id --fields _Chiller2RemainingUsefulLife,_id
find --table /apps/mqtt_records --where '{"$gt" : {"timestamp" : "1523079964"}}' --fields _Chiller2RemainingUsefulLife,timestamp
```

Here's an example of querying MQTT records table with Drill:

```
/opt/mapr/drill/drill-*/bin/sqlline -u jdbc:drill: -n mapr
    select * from dfs.`/apps/mqtt_records` limit 2;
```


## STEP 7 - Synthesize a high speed data stream:

This stream simulates time-series amplitudes of a vibration signal, at one sample every 10ms.
```
java -cp target/factory-iot-tutorial-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10
```

## STEP 8 - Process high speed data stream:

This will calculate FFTs on-the-fly for the high speed streaming data, and render an event in Grafana when FFTs changed more than 25% over a rolling window. This simulates anomaly detection for a vibration signal. Update "http://localhost:3000" with the hostname and port for your Grafana instance.

```
/opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 25.0 http://localhost:3000
```

## STEP 9 - Visualize data in Grafana

By now you should be able to see streaming MQTT data, vibration faults, and device failures in the Grafana dashboard.

![grafana dashboard](/images/grafana_screenshot.png?raw=true "Grafana Dashboard")

## STEP 10 - Open Jupyter
Open notebooks to see how Drill can be used to load MapR-DB data into notebooks for Deep Learning with Tensorflow.