package com.mapr.examples;

/******************************************************************************

PURPOSE: Generate a high speed sequence of amplitudes simulating a vibration signal and stream those amplitudes to a MapR Streams (or Kafka) topic in order to demonstrate high speed stream processing by something like Spark.

USAGE:
  /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class com.mapr.examples.StreamingFourierTransform target/factory-iot-tutorial-1.0-jar-with-dependencies.jar [/apps/mystream:mytopic] [interarrival time (ms)]

EXAMPLE:
java -cp target/factory-iot-tutorial-1.0-jar-with-dependencies.j com.mapr.examples.HighSpeedProducer /apps/mqtt:vibration 100
******************************************************************************/

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class HighSpeedProducer {
    private static Random rand = new Random();
    private static KafkaProducer producer;
    private static long records_processed = 0L;

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);

        // configure the producer options
        configureProducer();
        String topic = "/apps/mqtt:vibration";
        if (args.length > 0)
            topic = args[0];
        System.out.println("Sending messages to stream " + topic + "...");
        int delay = 1;
        if (args.length > 1)
            delay = Integer.parseInt(args[1]);
        System.out.println("Delaying " + delay + "ms between messages.");
        long startTime = System.nanoTime();
        long last_update = 0;
        try {
            while (true) {
                // Throttle a little bit to avoid unnecessary resource utilization.
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // Number of sample points for simulated vibration signal
                double N = 600;
                // Sample spacing for simulated vibration signal
                double T = 1.0 / 800.0;
                double phase_variation = rand.nextDouble();
                for (float i = 0; i < N*T; i += T) {
                    // We're simulating a vibration signal in the time-domain here,
                    // with amplitude 1 at 50(+/-5) Hz and amplitude 1/2 at 80(+/-5) Hz.
                    String value = Double.toString(1.0 * Math.sin(50 * 2 * Math.PI * i + phase_variation) + 0.5 * Math.sin(80 * 2 * Math.PI * (i) + phase_variation));
                    String key = Long.toString(System.nanoTime());
                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, key, "{\"t\":" + i + ",\"amplitude\":" + value + "}");
                    // Send the record to the producer client library.
                    producer.send(rec);
                    records_processed++;
                    // Print performance stats once per second
                    if ((Math.floor(System.nanoTime() - startTime) / 1e9) > last_update) {
                        last_update++;
                        producer.flush();
                        long elapsedTime = System.nanoTime() - startTime;
                        System.out.printf("Producer throughput = %d msgs/sec. Message count = %d\n", (int)(records_processed / ((double) elapsedTime / 1000000000.0)), records_processed);
                    }
                }
            }
        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
            System.out.println("Published " + records_processed + " messages to stream.");
            System.out.println("Finished.");
        }
    }

    public static void configureProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "required_but_does_not_matter:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }
}
