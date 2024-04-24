package com.simpletest.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ElasticSerachConsumer {

    public static RestHighLevelClient createClient(){
        String hostName = "kafka-course-2600926622.us-east-1.bonsaisearch.net";
        String password = "pzccro0pep";
        String userName = "s520cdmoh3";

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName,password));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName,443, "https")).setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                    @Override public HttpAsyncClientBuilder customizeHttpClient(
                            final HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String groupid = "kafka-demo-elasticsearch1";
        //String topic = "first_topic";
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to a new topic
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSerachConsumer.class);
        //String jsonString = "{\"foo\" : \"bar\"}";
        RestHighLevelClient client = createClient();


        KafkaConsumer<String,String> kafkaConsumer = createConsumer("first_topic");
        while(true) {
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            logger.info("Received"+ records.count() + "records");
            BulkRequest bulkRequest = new BulkRequest();
            for(ConsumerRecord<String,String> record : records){

                // two strategies to make consumer idempotent
                // generic id
                // twitter id
                try {
                    String id = record.topic() + record.partition() + record.offset();
                    // twitter feed specific id
                    //String id = extractIdFromTweet();
                    logger.info("key" + record.key());
                    logger.info("value" + record.value());
                    logger.info("partition" + record.partition());
                    logger.info("ofset" + record.offset());
                    String jsonString = "{\"value\" : \"" + record.value() + "\"}";
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                            .source(jsonString, XContentType.JSON);
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e){
                    logger.warn("bad data" + record.value());
                }
               // IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                //logger.info(response.getId());
               // Thread.sleep(1000);
            }
            BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            logger.info("Comitting offset");
            kafkaConsumer.commitAsync();
        }

    }
}
