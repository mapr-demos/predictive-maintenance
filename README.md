
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

<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/docker_config.png" width="50%">


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

<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/grafana_import.png" width="50%">

<hr>

# Predictive Maintenance Demo Procedure

For learning or debugging purposes you should run each of the following steps manually but if you just want to see data show up in Grafana then just run `./run.sh`.

## Step 1 - Simulate HVAC data stream:

We have provided a dataset captured from a real-world heating, ventilation, and air conditioning (HVAC) system in the `sample_dataset` folder. Run the following command to replay that HVAC stream to `/apps/factory:mqtt`.

```
cat ~/predictive-maintenance/sample_dataset/mqtt.json | while read line; do echo $line | sed 's/{/{"timestamp":"'$(date +%s)'",/g' | /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --topic /apps/factory:mqtt --broker-list this.will.be.ignored:9092; sleep 1; done
```

## Step 2 - Save IoT data stream to MapR-DB:

In the next step we'll save the IoT data stream to OpenTSDB so we can visualize it in Grafana, but in this step we save that stream to MapR-DB so we can apply labels necessary for supervised machine learning, as discussed in Step 4.

Run the following command to persist messages from stream `/apps/factory:mqtt` to MapR-DB table `/apps/mqtt_records`. 

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.MqttConsumer ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/factory:mqtt /apps/mqtt_records
```

Run this command to see how the row count increases:

```
/opt/mapr/drill/drill-*/bin/sqlline -u jdbc:drill: -n mapr
    select count(*) from dfs.`/apps/mqtt_records`;
```

## Step 3 - Save IoT data stream to OpenTSDB:

In this step we save the IoT data stream to OpenTSDB so we can visualize it in Grafana. 

Update `localhost:4242` with the hostname and port of your OpenTSDB server before running the following command:

```
/opt/mapr/kafka/kafka-*/bin/kafka-console-consumer.sh --new-consumer --topic /apps/factory:mqtt --bootstrap-server not.applicable:0000 | while read line; do echo $line | jq -r "to_entries | map(\"\(.key) \(.value | tostring)\") | {t: .[0], x: .[]} | .[]" | paste -d ' ' - - | awk '{system("curl -X POST --data \x27{\"metric\": \""$3"\", \"timestamp\": "$2", \"value\": "$4", \"tags\": {\"host\": \"localhost\"}}\x27 http://localhost:4242/api/put")}'; echo -n "."; done
```

After you have run that command you should be able to visualize the streaming IoT data. There are two ways to visualize this data:

1. ***Time-Series Dashboard*** - Depending on where you have installed Grafana, this can be opened with a URL like [http://maprdemo:3000](http://maprdemo:3000)

<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/grafana_screenshot_2.png" width="50%" align="center">

2. ***Factory Monitoring Software*** - This factory mock-up displays real-time HVAC data with an HTML file that should work in any web browser. `cd` to the `webapp/` directory, update the `OPENTSDB_HOST` variable at the top of [webapp/BuildingControl.html](https://github.com/mapr-demos/predictive-maintenance/blob/master/webapp/BuildingControl.html), then open it in a web browser. If you do not see data then be sure you have added the following three lines to `/opt/mapr/opentsdb/opentsdb-2.4.0/etc/opentsdb/opentsdb.conf`:

```
tsd.core.meta.enable_tsuid_incrementing = true
tsd.http.request.cors_domains=*
tsd.mode = ro/tsd.mode = rw
```

<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/BuildingControl.png" width="50%" align="center">


## Step 4 - Update lagging features in MapR-DB for each failure event:

This process will listen for failure events on a MapR Streams topic and retroactively label lagging features in MapR-DB when failures occur, as well as render the failure event in Grafana. Update "http://localhost:3000" with the hostname and port for your Grafana instance.

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.UpdateLaggingFeatures ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/factory:failures /apps/mqtt_records http://localhost:3000
```
<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/lagging_features_explanation.png" width="70%" align="center">

This particular step probably says the most about the value of MapR, because consider this: if you have a factory, instrumented by IoT devices reporting hundreds of metrics, per machine, per second, and you're tasked with the challenge of saving all that data until one day, often months into the future, you finally have a machine fail. At that point, you have to retroactively go back and update all those records as being "about to fail" or "x days to failure"  so that you can use that data for training models to predict those lagging features.  That's one heck of a DB update, right? The only way to store all that data is with a distributed database. This is what makes Spark and MapR-DB such a great fit. Spark - the distributed processing engine for big data, and MapR-DB - the distributed data store for big data, working together to process and store lots of data with speed and scalability. 

## Step 5 - Simulate a failure event:

To simulate a device failure, run this command:

```
echo "{\"timestamp\":"$(date +%s -d '60 sec ago')",\"deviceName\":\"Chiller1\"}" | /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --topic /apps/factory:failures --broker-list this.will.be.ignored:9092
```

This will trigger the Spark process you ran in the previous step to update lagging features in the MapR-DB table "/apps/mqtt_records". Once it sees the event you simulated, you should see it output information about the lagging features it labeled, like this:

![Update Lagging Features screenshot](/images/UpdateLagging_screenshot.png?raw=true "Update Lagging Features screenshot")


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

This command simulates a high speed data stream from a vibration sensor sampling once per 10ms.

```
java -cp ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar com.mapr.examples.HighSpeedProducer /apps/fastdata:vibrations 10
```

### Why are we simulating a vibration sensor?

Degradation in machines often manifests itself as a low rumble or a small shake. These unusual vibrations give you the first clue that a machine is nearing the end of its useful life, so it's very important to detect those anomalies. Vibration sensors measure the displacement or velocity of motion thousands of times per second. Analyzing those signals is typically done in the frequency domain. An algorithm called "fast Fourier transform" (FFT) can sample time-series vibration data and identify its component frequencies. In the next step you will run a command the converts the simulated vibration data to the frequency domain with an FFT and raises alarms when vibration frequencies vary more than a predefined threshold.

This demonstrates the capacity of MapR to ingest and process high speed streaming data. Depending on hardware, you will probably see MapR Streams processing more than 40,000 messages per second in this step.

## Step 8 - Process high speed data stream:

This will calculate FFTs on-the-fly for the high speed streaming data, and render an event in Grafana when FFTs changed more than 25% over a rolling window. This simulates anomaly detection for a vibration signal. Update "http://localhost:3000" with the hostname and port for your Grafana instance.

```
/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform ~/predictive-maintenance/target/predictive-maintenance-1.0-jar-with-dependencies.jar /apps/fastdata:vibrations 25.0 http://localhost:3000
```

## Step 9 - Visualize data in Grafana

By now you should be able to see streaming IoT data, vibration faults, and device failures in the Grafana dashboard.

![grafana dashboard](/images/grafana_screenshot.png?raw=true "Grafana Dashboard")

## Step 10 - Explore IoT data with Apache Drill

[Apache Drill](https://drill.apache.org/) is a Unix service that unifies access to data across a variety of data formats and sources. MapR is the primary contributor to Apache Drill, so naturally it is included in the MapR platform. Here's how you can use it to explore some of the IoT data we've gathered so far in this tutorial. 

Open the Drill web interface. If you're running MapR on your laptop then that's probably at [http://localhost:8047](http://localhost:8047). Here are a couple useful queries:

### Show how many messages have arrived cumulatively over days of the week:

```
SELECT _day_of_week_long, count(_day_of_week_long) FROM dfs.`/apps/mqtt_records` group by _day_of_week_long;
```
<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/drill_query_1.png" width="50%">
<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/drill_result_1.png" width="50%">

### Count how many faults have been detected:

```
WITH x AS
(
SELECT _id, _day_of_week_long, _Chiller1AboutToFail, ROW_NUMBER() OVER (PARTITION BY _Chiller1AboutToFail ORDER BY _id) as fault FROM dfs.`/apps/mqtt_records`
)
SELECT * from x WHERE _Chiller1AboutToFail = 'true' and fault = 1;
```
<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/drill_query_2.png" width="50%">
<img src="https://github.com/mapr-demos/predictive-maintenance/blob/master/images/drill_result_2.png" width="50%">


Drill can also be used to load data from MapR-DB into data science notebooks. Examples of this are shown in the following section.

## Step 11 (Optional) - Explore Machine Learning techniques for Predictive Maintenance

This tutorial focuses on data engineering - i.e. getting data in the right format and in the right place in order to take advantage of machine learning (ML) for predictive maintenance applications. The details of ML are beyond the scope of this tutorial but we've including a few python notebooks to illustrate common ML techniques for detecting anomalies and predicting machine failures using time-series data stored in flat files, OpenTSDB, and/or MapR-DB.

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

