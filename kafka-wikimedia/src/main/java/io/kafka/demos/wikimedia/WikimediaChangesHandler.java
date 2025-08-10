package io.kafka.demos.wikimedia;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesHandler implements EventHandler {

    String topic;
    KafkaProducer<String, String> producer;
    Logger logger = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    public WikimediaChangesHandler(String topic, KafkaProducer<String, String> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public void onOpen()  {
        //no change
    }

    @Override
    public void onClosed() {
        //close the producer when the event source is closed
        if (producer != null) {
            logger.info("Closing the producer...");
            producer.close();
        }
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        //send the message to the Kafka topic
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, messageEvent.getData());
        producer.send(record);
        logger.info("Produced record with value: {} to the topic: {}", messageEvent.getData(), topic);
    }

    @Override
    public void onComment(String s) throws Exception {
        //no change
    }

    @Override
    public void onError(Throwable throwable) {
        //log the error
        logger.error("Error occurred while processing the event: ", throwable);
    }
}
