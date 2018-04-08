This project shows how to build a data architecture for predictive maintenance of instrumented machinery. There are two primary data flows. One flow is intended to persist IoT data and label training data for sequence prediction and anomaly detection of time-series data in Tensorflow. The second flow is intended to persist time-series IoT data in OpenTSDB for visualization in a Grafana dashboard. 

![data flow diagram](/images/dataflow.png?raw=true "Data Flow")

Compile:

`mvn compile`

Build minimal jar:

`mvn package`

Build uber jar and copy dependencies to target/lib/:

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
echo "{\"timestamp\":"$(date +%s)",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --topic /apps/mqtt:failure --broker-list this.will.be.ignored:9092
```

Update Lagging features in mqtt table:

```
java -cp target/factory-iot-tutorial-1.0.jar:target/lib/* com.mapr.examples.UpdateLaggingFeatures /apps/mqtt:failure /tmp/iantest
```



Validate:

```
$ mapr dbshell
find --table /tmp/iantest --orderby _id --fields _Chiller2RemainingUsefulLife,_id
find --table /tmp/iantest --where '{"$gt" : {"_id" : "1523079964"}}' --orderby _id --fields _Chiller2RemainingUsefulLife,_id
find --table /tmp/iantest --where '{"$gt" : {"timestamp" : "1523079964"}}' --fields _Chiller2RemainingUsefulLife,timestamp
```