
# MapR for Predictive Maintenance

This project is intended to show how to build Predictive Maintenance applications on MapR. Predictive Maintenance applications place high demands on data streaming, time-series data storage, and machine learning. Therefore, this project focuses on data ingest with MapR Streams, time-series data storage with MapR-DB and OpenTSDB, and feature engineering with MapR-DB and Apache Spark.

# Overview:

Predictive Maintenance requires a cutting edge data platform in order to handle fast streams of  IoT data, with the processing required for on-the-fly feature engineering, and the flexibility required for data science and machine learning.

## Ingesting Factory IoT Data 

Predictive Maintenance applications rely heavily on ingesting multiple data sources, each with their own format and throughput. MapR Streams can ingest data, regardless of format or speed, with standard Kafka and RESTful APIs.

## Machine Learning on Factory IoT Data
 
The "predictive" aspects of Predictive Maintenance applications are usually realized through machine learning. Feature engineering is often considered the most important aspect of machine learning (as opposed to neural network design, for example). Feature engineering places the high demands on the data layer because the amount of data that IoT data streams generate. The tendency for failures to occur infrequently and without warning means vast amounts of raw time-series data must be stored. Not only must it be stored, but it must also be possible to retroactively update the lagging features necessary in order to label failures for the purposes of supervised machine learning. MapR-DB and Spark can work together to provide the capabiltieis required to put machine learning into practice for predictive maintance.

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

These steps explain how to setup this tutorial using the [MapR Container for Developers](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html) on MacOS.

## Allocate 12GB to Docker

This tutorial requires a lot of memory. We recommend allocating 12GB RAM, 4GB swap, and 2 CPUs to the Docker Community Edition for MacOS.

![docker config](/images/docker_config.png?raw=true "Docker Config")

## Start the MapR sandbox

Download and run the `./mapr_devsandbox_container_setup.sh` script.

```
git clone https://github.com/mapr-demos/mapr-db-60-getting-started
cd mapr-db-60-getting-started
./mapr_devsandbox_container_setup.sh
```

## Run the `init.sh` script

Run the `init.sh` script to install Spark, OpenTSDB, Grafana, and some other things necessary to use the sample applications in this tutorial. SSH to the sandbox container, with password "mapr" and run the following commands. This should take about 20 minutes.

```
ssh -p 2222 root@localhost
wget https://raw.githubusercontent.com/mapr-demos/predictive-maintenance/master/init.sh
chmod 700 ./init.sh
sudo ./init.sh
```

## Import the Grafana dashboard

```
sudo /opt/mapr/server/configure.sh -R -OT `hostname -f`
sudo /opt/mapr/opentsdb/opentsdb-2.4.0/etc/init.d/opentsdb start
```

Open Grafana data sources, with a URL like [http://maprdemo:3000/datasources/edit/1](http://maprdemo:3000/datasources/edit/1), and add OpenTSDB as a new data source.

Load the `Grafana/IoT_dashboard.json` file using Grafana's dashboard import functionality, and specify "MaprMonitoringOpenTSDB" as the data source, as shown below:

![grafana import](/images/grafana_import.png?raw=true "Grafana Import") 


<hr>

# Predictive Maintenance Demo Procedure

For learning or debugging purposes you should run each of the following steps manually but if you just want to see data show up in Grafana then just run `./run.sh`.

## Step 1 - Simulate Factory IoT data stream:

This will stream 150 metrics once every couple of seconds to `/apps/factory:mqtt`.

```
cat ~/predictive-maintenance/sample_dataset/mqtt.json | while read line; do echo $line | sed 's/{/{"timestamp":"'$(date +%s)'",/g' | /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --topic /apps/factory:mqtt --broker-list this.will.be.ignored:9092; sleep 1; done
```

## Step 2 - Save IoT data stream to MapR-DB:

This will persist messages from stream `/apps/factory:mqtt` to MapR-DB table `/apps/mqtt_records`. 

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.MqttConsumer ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/factory:mqtt /apps/mqtt_records
```

Run this command to see how the row count increases:

```
/opt/mapr/drill/drill-*/bin/sqlline -u jdbc:drill: -n mapr
    select count(*) from dfs.`/apps/mqtt_records`;
```

## Step 3 - Save IoT data stream to OpenTSDB:

In order to see data in the Grafana dashboard, we need to write data to OpenTSDB. Here's how to continuously save the stream `/apps/factory:mqtt` to OpenTSDB:

Update `localhost` with the hostname of the node running OpenTSDB.

```
/opt/mapr/kafka/kafka-*/bin/kafka-console-consumer.sh --new-consumer --topic /apps/factory:mqtt --bootstrap-server not.applicable:0000 | while read line; do echo $line | jq -r "to_entries | map(\"\(.key) \(.value | tostring)\") | {t: .[0], x: .[]} | .[]" | paste -d ' ' - - | awk '{system("curl -X POST --data \x27{\"metric\": \""$3"\", \"timestamp\": "$2", \"value\": "$4", \"tags\": {\"host\": \"localhost\"}}\x27 http://localhost:4242/api/put")}'; echo -n "."; done
```

## Step 4 - Update lagging features in MapR-DB for each failure event:

This process will listen for failure events on a MapR Streams topic and retroactively label lagging features in MapR-DB when failures occur, as well as render the failure event in Grafana. Update "http://localhost:3000" with the hostname and port for your Grafana instance.

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/factory:failures /apps/mqtt_records http://localhost:3000
```

This particular step probably says the most about the value of MapR, because consider this: if you have a factory, instrumented by IoT devices reporting hundreds of metrics, per machine, per second, and you're tasked with the challenge of saving all that data until one day, often months into the future, you finally have a machine fail. At that point, you have to retroactively go back and update all those records as being "about to fail" or "x days to failure"  so that you can use that data for training models to predict those lagging features.  That's one heck of a DB update, right? The only way to store all that data is with a distributed database. This is what makes Spark and MapR-DB such a great fit. Spark - the distributed processing engine for big data, and MapR-DB - the distributed data store for big data, working together to process and store lots of data with speed and scalability. 

## Step 5 - Simulate a failure event:

```
echo "{\"timestamp\":"$(date +%s -d '60 sec ago')",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --topic /apps/factory:failures --broker-list this.will.be.ignored:9092
```

## Step 6 - Validate that lagging features have been updated:

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

Here's an example of querying IoT data records table with Drill:

```
/opt/mapr/drill/drill-*/bin/sqlline -u jdbc:drill: -n mapr
    select * from dfs.`/apps/mqtt_records` limit 2;
```


## Step 7 - Synthesize a high speed data stream:

This stream simulates time-series amplitudes of a vibration signal, at one sample every 10ms.
```
java -cp ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10
```

## Step 8 - Process high speed data stream:

This will calculate FFTs on-the-fly for the high speed streaming data, and render an event in Grafana when FFTs changed more than 25% over a rolling window. This simulates anomaly detection for a vibration signal. Update "http://localhost:3000" with the hostname and port for your Grafana instance.

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 25.0 http://localhost:3000
```

## Step 9 - Visualize data in Grafana

By now you should be able to see streaming IoT data, vibration faults, and device failures in the Grafana dashboard.

![grafana dashboard](/images/grafana_screenshot.png?raw=true "Grafana Dashboard")

## Step 10 (Optional) - Explore Machine Learning techniques for Predictive Maintenance

This tutorial focuses on data engineering - i.e. getting data in the right format and in the right place in order to take advantage of machine learning (ML) for predictive maintenance applications. The details of ML are beyond the scope of this tutorial but we've including a few python notebooks to illustrate common ML techniques for detecting anomolies and predicting machine failures using time-series data stored in flat files, OpenTSDB, and/or MapR-DB.

### LSTM predictions for "About To Fail"

This notebook shows how to train a binary classification model using an LSTM neural network in Keras to predict whether a failure will occur in an airplane engine within the next 30 seconds.
![lstm-about_to_fail](/images/lstm-about_to_fail.png?raw=true "LSTM Binary Classification Model")

### LSTM predictions for "Remaining Useful Life"

This notebook shows how to train a point regression model using an LSTM neural network in Keras to predict the point in time when an airplane engine will fail.
![lstm-rul](/images/lstm-rul.png?raw=true "LSTM Point Regression Model")

### LSTM time series predictions from OpenTSDB

This notebook shows how to train a model to predict the next value in a sequence of numbers, which in this case, is a time series sequence of numbers stored in OpenTSDB. This notebook also shows how to load training data into Tensorflow using the REST API for OpenTSDB.
![lstm-matplotlib](/images/lstm-matplotlib.png?raw=true "LSTM Time-Series Forecasting for OpenTSDB")

### RNN time series predictions from OpenTSDB

This notebook is the same as the last one, except it shows how to build an RNN model instead LSTM for predicting future values of time-series data in OpenTSDB.
![rnn-matplotlib](/images/rnn-matplotlib.png?raw=true "RNN Time-Series Forecasting for OpenTSDB")

### RNN predictions for MapR-DB data via Drill

This notebook is the same as the last one, except it loads time-series data from MapR-DB instead of OpenTSDB.
![rnn-zeppelin](/images/rnn-zeppelin.png?raw=true "RNN Time-Series Forecasting for MapR-DB")

