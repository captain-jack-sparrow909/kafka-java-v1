package io.kafka.demos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        logger.info("Starting Kafka Producer Demo with Callbacks...");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the KafkaProducer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java_topic", "Hello, Kafka with Callback!");

        //send the record
        producer.send(record, ((recordMetadata, e) -> {
            //executes every time a record is successfully sent or an exception occurs
            if(e == null){
                logger.info("Received new metadata. \nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
            }
            else {
                logger.error("Error while producing", e);
            }
        }));

        //flush and close the producer
        producer.flush();  //ensure all records are sent before closing, synchronously
        producer.close();
    }
}
