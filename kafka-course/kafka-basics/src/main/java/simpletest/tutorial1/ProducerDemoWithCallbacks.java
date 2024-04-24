package simpletest.tutorial1;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {
        // create logger
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);
        System.out.println("Hello world kafka");
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);
        // craete record
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
                    "Hello_world" + ":" + String.valueOf(i));
            // send data
            producer.send(record, new Callback() {
                @Override public void onCompletion(final RecordMetadata recordMetadata, final Exception e) {
                    if (e == null) {
                        logger.info("Received meta data info \n" +
                                "topic : " + recordMetadata.topic() + "\n"
                                + "partition : " + recordMetadata.partition() + "\n"
                                + "offset : " + recordMetadata.offset() + "\n"
                                + "timestamp : " + recordMetadata.timestamp());
                        logger.info(String.valueOf(recordMetadata.partition()));
                        logger.info(String.valueOf(recordMetadata.hasOffset()));
                        logger.info(String.valueOf(recordMetadata.timestamp()));
                    } else {
                        logger.error("logging error" + e.getMessage());
                    }
                }
            });

            producer.send(record);
        }
            // producer flush the data
            producer.flush();
            producer.close();
        }

}
