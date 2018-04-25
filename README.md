
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

## Step 1 - Start the MapR sandbox

Download and run the `./mapr_devsandbox_container_setup.sh` script.

```
git clone https://github.com/mapr-demos/mapr-db-60-getting-started
cd mapr-db-60-getting-started
./mapr_devsandbox_container_setup.sh
```

## Step2 - Reduce memory usage required by the sandbox

SSH to the sandbox container, with password "mapr":

```
ssh -p 2222 root@localhost
```

Remove Hive and reconfigure Drill to use less memory:

```
sudo apt-get remove mapr-hive mapr-spark -y
```
	
## Step 2 - Reconfigure the MapR sandbox

Install full versions of Spark and Kafka on the sandbox. This should take about 5 minutes.

```
sudo apt-get install maven git jq -y
sudo apt-get install mapr-spark mapr-spark-master mapr-spark-historyserver mapr-spark-thriftserver mapr-kafka -y 
cp /opt/mapr/spark/spark-2.2.1/conf/slaves.template /opt/mapr/spark/spark-2.2.1/conf/slaves
sudo /opt/mapr/server/configure.sh -R
```

Set the local timezone so Grafana timestamps 

```
sudo apt-get --download-only install tzdata
sudo dpkg -i  /var/cache/apt/archives/tzdata*.deb
sudo rm /etc/localtime
sudo ln -s /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
```

## Install OpenTSDB and Grafana:

```
sudo apt-get install mapr-opentsdb -y
sudo apt-get install mapr-grafana -y
```

Enabled write access to opentsdb:

```
sudo cat /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf | sed "s/#tsd.mode = ro/tsd.mode = rw/g" > /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf.new
sudo mv /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf.new /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf
sudo chown mapr:mapr /opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf
```

Start OpenTSDB and Grafana:

```
sudo /opt/mapr/server/configure.sh -R -OT `hostname -f`
sudo /opt/mapr/opentsdb/opentsdb-2.4.0/etc/init.d/opentsdb start
```

Open Grafana data sources, with a URL like [http://maprdemo:3000/datasources/edit/1](http://maprdemo:3000/datasources/edit/1), and add OpenTSDB as a new data source.

Load the `Grafana/IoT_dashboard.json` file using Grafana's dashboard import functionality, and specify "MaprMonitoringOpenTSDB" as the data source, as shown below:

![grafana import](/images/grafana_import.png?raw=true "Grafana Import") 

# Compile the demo code:

Compile the code on the mapr cluster. This could take up to 15 minutes.

```
git clone https://github.com/mapr-demos/factory-iot-tutorial
cd factory-iot-tutorial
mvn package
```

## Create streams:

On the mapr cluster:
 
``` 
maprcli stream create -path /apps/factory -produceperm p -consumeperm p -topicperm p -ttl 900 -json
maprcli stream topic create -path /apps/factory -topic mqtt -partitions 1 -json
maprcli stream topic create -path /apps/factory -topic failures -partitions 1 -json
maprcli stream create -path /apps/fastdata -produceperm p -consumeperm p -topicperm p -ttl 900 -json
maprcli stream topic create -path /apps/fastdata -topic vibrations -partitions 1 -json
```

# Predictive Maintenance Demo Procedure

## STEP 1 - Simulate raw IoT data stream:

This will stream 150 metrics once every couple of seconds to `/apps/factory:mqtt`.

```
cd sample_dataset
gunzip mqtt.json.gz
cat mqtt.json | while read line; do echo $line | sed 's/{/{"timestamp":"'$(date +%s)'",/g' | /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --topic /apps/factory:mqtt --broker-list this.will.be.ignored:9092; echo -n "."; sleep 1; done
```

## STEP 2 - Save MQTT stream to MapR-DB:

This will persist messages from stream `/apps/factory:mqtt` to MapR-DB table `/apps/mqtt_records`. 

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.MqttConsumer target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/factory:mqtt /apps/mqtt_records
```

Run this command to see how the row count increases:

```
/opt/mapr/drill/drill-*/bin/sqlline -u jdbc:drill: -n mapr
    select count(*) from dfs.`/apps/mqtt_records`;
```

## STEP 3 - Save MQTT stream to OpenTSDB:

In order to see data in the Grafana dashboard, we need to write data to OpenTSDB. Here's how to continuously save the stream `/apps/factory:mqtt` to OpenTSDB:

Update `localhost` with the hostname of the node running OpenTSDB.

```
/opt/mapr/kafka/kafka-*/bin/kafka-console-consumer.sh --topic /apps/factory:mqtt --new-consumer --bootstrap-server not.applicable:0000 | while read line; do echo $line | jq -r "to_entries | map(\"\(.key) \(.value | tostring)\") | {t: .[0], x: .[]} | .[]" | paste -d ' ' - - | awk '{system("curl -X POST --data \x27{\"metric\": \""$3"\", \"timestamp\": "$2", \"value\": "$4", \"tags\": {\"host\": \"localhost\"}}\x27 http://localhost:4242/api/put")}'; echo -n "."; done
```

## STEP 4 - Update lagging features in MapR-DB for each failure event:

This process will listen for failure events on a MapR Streams topic and retroactively label lagging features in MapR-DB when failures occur, as well as render the failure event in Grafana. Update "http://localhost:3000" with the hostname and port for your Grafana instance.

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/factory:failures /apps/mqtt_records http://localhost:3000
```

## STEP 5 - Simulate a failure event:

```
echo "{\"timestamp\":"$(date +%s -d '60 sec ago')",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --topic /apps/factory:failures --broker-list this.will.be.ignored:9092
```

## STEP 6 - Validate that lagging features have been updated:

```
$ mapr dbshell
find /apps/mqtt_records --where '{ "$eq" : {"_Chiller1AboutToFail":"true"} }' --f _id,_Chiller1AboutToFail,timestamp
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
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform target/factory-iot-tutorial-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 25.0 http://localhost:3000
```

## STEP 9 - Visualize data in Grafana

By now you should be able to see streaming MQTT data, vibration faults, and device failures in the Grafana dashboard.

![grafana dashboard](/images/grafana_screenshot.png?raw=true "Grafana Dashboard")

## STEP 10 (Optional) - Explore Machine Learning techniques for Predictive Maintenance

The focus of this tutorial is data engineering (aka feature engineering), and what you need to have in order to take advantage of machine learning (ML) for the purposes of predictive maintenance. Even though the details of ML are beyond the scope of this tutorial, it would not be complete without including a few ML examples. We've included several python notebooks have been provided to show how to load data from MapR into notebooks for Machine Learning with Tensorflow. 

* ***LSTM predictions for "About To Fail"*** - This notebook shows how to train a model using an LSTM neural network in Keras to predict whether a failure will occur in an airplane engine within the next 30 seconds. This type of classification is called, "binary classification" and the model is trained using labeled data, meaning this is "supervised learning", where the value being predicted is either true or false.  
* ***LSTM predictions for "Remaining Useful Life"*** - This notebook shows how to train a model using an LSTM neural network in Keras to predict when an airplane engine will fail. The model is trained using labeled data, meaning this is "supervised learning", where the value being predicted is a number representing the time left before an airplane engine fails.
* ***LSTM time series predictions from OpenTSDB*** - This notebook shows how to train a model to predict the next value in a sequence of numbers, which in this case, is a time series sequence of numbers stored in OpenTSDB. Not only does this notebook show how to train the LSTM model, but it also shows how to load training data into Tensorflow using the REST API for OpenTSDB.
* ***RNN time series predictions from OpenTSDB*** - This notebook shows how to train a model to predict the next value in a sequence of numbers, which in this case, is a time series sequence of numbers stored in OpenTSDB. Not only does this notebook show how to train an RNN model, but it also shows how to load training data into Tensorflow using the REST API for OpenTSDB.
* RNN predictions for MapR-DB data via Drill
 
In order to run these examples you'll need keras and sklearn, which can be installed like this:

```
pip install keras sklearn
```

