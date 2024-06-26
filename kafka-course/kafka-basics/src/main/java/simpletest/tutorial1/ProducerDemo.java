package simpletest.tutorial1;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class ProducerDemo {
    public static void main(String[] args) {
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
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello_world");
        // send data
        producer.send(record);
        // producer flush the data
        producer.flush();
        producer.close();

    }
}
