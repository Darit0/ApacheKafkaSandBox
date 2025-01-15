package sandbox;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaProducerAPI {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerAPI.class);
    private static RecordMetadata metadata;

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)){
//            producer.initTransactions();
//
//            producer.beginTransaction();
            producer.send(
                   new ProducerRecord<>("sandbox", "Hello World gnomiki"),
                    (metadata, exception)-> LOGGER.info("Metadata: {}", metadata)).get();

            //throw new IllegalArgumentException();
            //producer.commitTransaction();

        }
    }

}
