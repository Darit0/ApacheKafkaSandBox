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

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)){
            metadata = producer.send(
                   new ProducerRecord<String, String>("sandbox", "Hello World gnomiki")).get();
            LOGGER.info("=================================");
            LOGGER.info("MetaData: {}", metadata);
            LOGGER.info("=================================");

            metadata = producer.send(
                    new ProducerRecord<String, String>("sandbox", "My-key","Hello World gnomiki, i have key")).get();
            LOGGER.info("=================================");
            LOGGER.info("MetaData: {}", metadata);
            LOGGER.info("=================================");

            metadata = producer.send(
                    new ProducerRecord<String, String>("sandbox", 0,"My-key",
                            "Hello World gnomiki, i have key and partion")).get();
            LOGGER.info("=================================");
            LOGGER.info("MetaData: {}", metadata);
            LOGGER.info("=================================");

            metadata = producer.send(
                    new ProducerRecord<String, String>("sandbox", 0,"My-key",
                            "Hello World gnomiki, i have key, parition",
                            List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();

            LOGGER.info("=================================");
            LOGGER.info("MetaData: {}", metadata);
            LOGGER.info("=================================");

            metadata = producer.send(
                    new ProducerRecord<String, String>("sandbox", 1, System.currentTimeMillis(),
                            "My-key-with-timestamp",
                            "Hello World gnomiki, i have key and partion")).get();
            LOGGER.info("=================================");
            LOGGER.info("MetaData: {}", metadata);
            LOGGER.info("=================================");

            metadata = producer.send(
                    new ProducerRecord<String, String>("sandbox", 1, System.currentTimeMillis(),
                            "My-key-with-timestamp",
                            "Hello World gnomiki, i have key, parition",
                            List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();

            LOGGER.info("=================================");
            LOGGER.info("MetaData: {}", metadata);
            LOGGER.info("=================================");

            metadata = producer.send(
                    new ProducerRecord<>("sandbox", "Hello World gnomiki"),
                    (md, exception)->{LOGGER.info("Callback: metadata: {}, exception == null{}", md, exception==null);
                    }).get();
            LOGGER.info("=================================");
            LOGGER.info("MetaData: {}", metadata);
            LOGGER.info("=================================");
        }
    }

}
