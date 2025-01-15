package sandbox;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;



public class KafkaConsumerAPI {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerAPI.class);

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
       properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerAPI");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {

//            consumer.assign(List.of(
//                    new TopicPartition("sandbox", 0),
//                    new TopicPartition("sandbox", 1),
//                    new TopicPartition("sandbox", 2)
//            ));
            consumer.subscribe(Pattern.compile("sandbox"), new MyConsumerRebalanceListener());

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            StreamSupport.stream(records.spliterator(), false)
                    .forEach(record -> LOGGER.info("Record: {}", record));

        }
    }
}

class MyConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerAPI.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("onPartitionsRevoked: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.info("onPartitionsAssigned: {}", partitions);
    }
}
