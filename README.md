
# How to use MapR for Predictive Maintenance Applications

MapR is a good platform to use for Predictive Maintenance because it has the following charactersistics:
* MapR Streams provide a convenient way to ingest IoT data, which is often sampled frequently, and never ends. (i.e. it's a stream)
* The integration of MapR DB with Spark provides a convenient way to label lagging features needed for predicting failures via supervised Machine Learning.
* Drill provides a convenient way to load ML data sets into Tensorflow for unsupervised and supervised machine learning
 
This project is intended to show how to build a data architecture for predictive maintenance using MapR. There are two primary data flows. 

1. One flow is intended to persist IoT data and label training data for sequence prediction and anomaly detection of time-series data in Tensorflow. 
2. The second flow is intended to persist time-series IoT data in OpenTSDB for visualization in a Grafana dashboard. 

![data flow diagram](/images/dataflow.png?raw=true "Data Flow")

# Usage

Download:

`git clone https://github.com/mapr-demos/factory-iot-tutorial`

Compile:

`mvn compile`

Build minimal jar:

`mvn package`

Copy dependencies to target/lib/:

`mvn install`

Create streams:

```
maprcli stream create -path /apps/mqtt -produceperm p -consumeperm p -topicperm p -ttl 900 -json
maprcli stream topic create -path /apps/mqtt -topic opto22 -partitions 1 -json
maprcli stream topic create -path /apps/mqtt -topic failures -partitions 1 -json

```

Synthesize mqtt stream:

```
cat mqtt.json | while read line; do echo $line | sed 's/{/{"timestamp":"'$(date +%s)'",/g' | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:opto22 --broker-list this.will.be.ignored:9092; echo -n "."; sleep 1; done
```

Persist mqtt stream to mapr-db:

```
java -cp target/factory-iot-tutorial-1.0.jar:target/lib/* com.mapr.examples.MqttConsumer /apps/mqtt:opto22 /tmp/iantest
```

Synthesize failure stream:

```
echo "{\"timestamp\":"$(date +%s -d '60 sec ago')",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:failure --broker-list this.will.be.ignored:9092
```

Update Lagging features in mqtt table:

```
java -cp target/factory-iot-tutorial-1.0.jar:target/lib/* com.mapr.examples.UpdateLaggingFeatures /apps/mqtt:failure /tmp/iantest
```

Then validate that those lagging features have been updated:

```
$ mapr dbshell
find /tmp/iantest --where '{ "$eq" : {"_Chiller1AboutToFail":true} }' --f _id,_Chiller1AboutToFail,timestamp
find /tmp/iantest --where '{ "$eq" : {"timestamp":"1523339687"} }' --f _id,_Chiller1AboutToFail,timestamp
```

Here are a few examples to validate the database with dbshell:

```
$ mapr dbshell
find --table /tmp/iantest --orderby _id --fields _Chiller2RemainingUsefulLife,_id
find --table /tmp/iantest --where '{"$gt" : {"_id" : "1523079964"}}' --orderby _id --fields _Chiller2RemainingUsefulLife,_id
find --table /tmp/iantest --where '{"$gt" : {"timestamp" : "1523079964"}}' --fields _Chiller2RemainingUsefulLife,timestamp
```