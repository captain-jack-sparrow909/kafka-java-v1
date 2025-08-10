package io.kafka.demos.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "localhost:9092";
        String topic = "wikimedia.recentchange";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServer);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create EventHandler
        EventHandler eventHandler = new WikimediaChangesHandler(topic, producer);

        //Wikimedia URL:
        String wikimediaUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
        //create event source
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(wikimediaUrl));
        EventSource eventSource = builder.build();

        //start the event source
        eventSource.start();

        // Keep the application running to listen for events
        TimeUnit.MINUTES.sleep(10); // Keep the application running for 10 minutes
        //Without it, main() would finish instantly and close the producer before it can send anything.
        //“Run for 10 minutes, then pull the plug — no matter what’s in progress.”
        //if you want to run it indefinitely, you can use a while loop like this:
        // while (true) {
        //     TimeUnit.SECONDS.sleep(1); // Sleep for 1 second to avoid busy-waiting
        // }
    }
}
