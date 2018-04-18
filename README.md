
# How to use MapR for Predictive Maintenance Applications
ls ju
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
maprcli stream delete -path /apps/mqtt 
maprcli stream create -path /apps/mqtt -produceperm p -consumeperm p -topicperm p -ttl 900 -json
maprcli stream topic create -path /apps/mqtt -topic opto22 -partitions 1 -json
maprcli stream topic create -path /apps/mqtt -topic failures -partitions 1 -json
maprcli stream create -path /apps/fastdata -produceperm p -consumeperm p -topicperm p -ttl 900 -json
maprcli stream topic create -path /apps/fastdata -topic vibrations -partitions 1 -json

```

Synthesize mqtt stream:

```
cat mqtt.json | while read line; do echo $line | sed 's/{/{"timestamp":"'$(date +%s)'",/g' | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:opto22 --broker-list this.will.be.ignored:9092; echo -n "."; sleep 1; done
```

Persist mqtt stream to MapR-DB:

```
java -cp target/factory-iot-tutorial-1.0.jar:target/lib/* com.mapr.examples.MqttConsumer /apps/mqtt:opto22 /apps/mqtt_records
```

Persist mqtt stream to OpenTSDB:
/opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-consumer.sh --topic /apps/mqtt:opto22 --new-consumer --bootstrap-server not.applicable:0000 | while read line; do echo $line | jq -r "to_entries | map(\"\(.key) \(.value | tostring)\") | {t: .[0], x: .[]} | .[]" | paste -d ' ' - - | awk '{system("curl -X POST --data \x27{\"metric\": \""$3"\", \"timestamp\": "$2", \"value\": "$4", \"tags\": {\"host\": \"localhost\"}}\x27 http://localhost:4242/api/put")}'; echo -n "."; done


Synthesize failure stream:

```
echo "{\"timestamp\":"$(date +%s -d '60 sec ago')",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:failures --broker-list this.will.be.ignored:9092
```

Update Lagging features in mqtt table:

```
java -cp target/factory-iot-tutorial-1.0.jar:target/lib/* com.mapr.examples.UpdateLaggingFeatures /apps/mqtt:failures /apps/mqtt_records
```

Then validate that those lagging features have been updated:

```
$ mapr dbshell
find /apps/mqtt_records --where '{ "$eq" : {"_Chiller1AboutToFail":true} }' --f _id,_Chiller1AboutToFail,timestamp
find /apps/mqtt_records --where '{ "$eq" : {"timestamp":"1523339687"} }' --f _id,_Chiller1AboutToFail,timestamp
```

Here are a few examples to validate the database with dbshell:

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


Synthesize a high speed data stream:

```
java -cp target/factory-iot-tutorial-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10
```

Process high speed data stream:

```
/opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 25.0
```