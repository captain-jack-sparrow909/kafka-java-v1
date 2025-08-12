package io.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class DemoConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DemoConsumer.class.getSimpleName());
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

        //create the KafkaProducer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to the topic
        consumer.subscribe(Arrays.asList(topic));  //you can subscribe to multiple topics by passing a list

        //poll for new records
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //will wait for 1 second for new records
            //if no new records are found, it will return an empty ConsumerRecords object, and the loop will continue, it'll wait for the next poll
//            "Ask Kafka for any available records, and wait up to 1000 milliseconds (1 second) for them to arrive."
//            In this case, it will:
//                Return immediately if there are records already in the buffer.
//                        Wait up to 1 second for new records if none are available.
//                        Return an empty ConsumerRecords if still nothing after 1 second.

            for(ConsumerRecord<String, String> record : records){
                //process each record
                logger.info("Received new record: \nKey: {}\nValue: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                        record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
            }
        }
    }
}
