package io.kafka.demos;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        String bootstrapServers = "localhost:9092";
        String groupId = "consumer-opensearch-demo";
        String topic = "wikimedia.recentchange";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);
    }

    public static String extractId(String json){
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //create open search client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //subscribe to the topic
        String topic = "wikimedia.recentchange";
        consumer.subscribe(Arrays.asList(topic));

        //adding shut down hook:
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Received shutdown signal, closing the consumer and OpenSearch client");
            consumer.wakeup(); //this will throw an exception in the poll() method, which will break the loop and allow us to close the consumer
            try {
                mainThread.join();
            }
            catch (InterruptedException e) {
                logger.error("Error while waiting for main thread to finish: ", e);
            }
        }));

        //creating index on open search client:
        try(openSearchClient; consumer){  //this will automatically close the client when done
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if(!indexExists){
                logger.info("Index doesn't exist, creating it now...");
                openSearchClient.indices().create(new CreateIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            } else {
                logger.info("Index already exists, skipping creation.");
            }

            BulkRequest bulkRequest = new BulkRequest();

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //will wait for 1 second for new records
                records.forEach(record -> {
                    //making our system idempotent by using the record key as the document ID in OpenSearch
                    //approach 1:
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    //approch 2: extract id from the payload
                    String id = extractId(record.value());

                    //send the record to open search
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
                    //with the id added, now if we try to index the same record again, it will not create a duplicate document, but will update the existing one.
                    //if you want to create a new document every time, you can remove the id from the index request, but your system will not be idempotent anymore.

                    bulkRequest.add(indexRequest);

                    //below is approach for a single request, which is inefficient, instead we can use bulk requests to send multiple records at once
//                    try {
//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                        logger.info("Record id: {} indexed to OpenSearch: \nKey: {}\nValue: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",response.getId(),
//                                record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
//                    } catch (IOException e) {
//                        logger.error("Error indexing record to OpenSearch: ", e);
//                    }
                });

                //send the bulk request to OpenSearch
                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Bulk request sent to OpenSearch with {} actions.", bulkRequest.numberOfActions());
                    try {
                        Thread.sleep(1000); // sleep for 1 second to simulate processing time, and increase our chances of batching more records together
                        //this is not must though
                    } catch (InterruptedException e) {
                        logger.error("Thread interrupted while sleeping: ", e);
                    }
                    if(bulkResponse.hasFailures()) {
                        logger.error("Bulk request failed with errors: {}", bulkResponse.buildFailureMessage());
                    } else {
                        logger.info("Bulk request successful, indexed {} records.", bulkResponse.getItems().length);
                    }
                }
            }

        } catch (WakeupException e){
            //this exception is thrown when the consumer.wakeup() method is called
            //it is used to interrupt the consumer.poll() method and allow the main thread to exit gracefully
            logger.info("Consumer wakeup exception caught. Exiting...");
        } catch (IOException e) {
            logger.error("Error occurred while creating OpenSearch client or indexing records: ", e);
        } finally {
            consumer.close();
            logger.info("Kafka consumer closed.");
            openSearchClient.close();
            logger.info("OpenSearch client closed.");
        }
    }
}

