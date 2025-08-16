package io.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class DemoConsumerWithShutdown {
    private static final Logger logger = LoggerFactory.getLogger(DemoConsumerWithShutdown.class.getSimpleName());
    public static void main(String[] args) {
        logger.info("Starting Kafka Consumer Demo...");

        String topic = "demo_java_topic";
        String groupId = "demo_java_group";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //start reading from the earliest offset if no previous offset is found
        //if you want to start reading from the latest offset, set it to "latest"
        //auto.offset.reset has other options like "none" (if no previous offset is found, it will throw an exception)
        // and "earliest" (if no previous offset is found, it will start reading from the earliest offset)

        //create the KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered. Closing consumer ");
            consumer.wakeup();  //this will interrupt the consumer.poll() method and allow the main thread to exit gracefully

            try {
                mainThread.join(); //wait for the main thread to finish
            } catch (InterruptedException e) {
                logger.error("Error while waiting for main thread to finish", e);
            }
        }));
        //This hook is registered with the JVM to execute when the application receives a termination signal, such as:
        //Pressing Ctrl+C in terminal.
        //System shutdown.
        //kill signal (not kill -9).
        //Any normal termination of the JVM.
        //This shutdown hook does not run immediately, but is invoked only when the JVM begins shutting down.


        try {
            //subscribe to the topic
            consumer.subscribe(Arrays.asList(topic));  //you can subscribe to multiple topics by passing a list

            //poll for new records
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //will wait for 1 second for new records
                //if no new records are found, it will return an empty ConsumerRecords object, and the loop will continue, it'll wait for the next poll
                for(ConsumerRecord<String, String> record : records){
                    //process each record
                    logger.info("Received new record: \nKey: {}\nValue: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                            record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
                }
            }
        } catch (WakeupException e){
            //this exception is thrown when the consumer.wakeup() method is called
            //it is used to interrupt the consumer.poll() method and allow the main thread to exit gracefully
            logger.info("Consumer wakeup exception caught. Exiting...");
        } catch (Exception e) {
            logger.error("Error while consuming records", e);
        } finally {
            consumer.close();  //close the consumer to release resources, this will also commit the offsets of the records that were processed
            logger.info("Consumer closed.");
        }

    }
}
