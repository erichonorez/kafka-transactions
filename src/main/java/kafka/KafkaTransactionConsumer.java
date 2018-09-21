package kafka;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaTransactionConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());

        consumer.subscribe(Collections.singleton("tests"));
        // should only be called once
        producer.initTransactions();

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(r -> {
                // Do something here like update a state in DB or publishing in another topic
                System.out.println(
                    MessageFormat.format("key:{0} value:{1}", r.key(), r.value())
                );
            });
            producer.beginTransaction();
            producer.sendOffsetsToTransaction(consumerOffsets(records), "kafka-app-test-consumer");
            producer.commitTransaction();
            //producer.abortTransaction();
        }

    }

    private static Map<TopicPartition, OffsetAndMetadata> consumerOffsets(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        Iterator<TopicPartition> iterator = records.partitions().iterator();
        while (iterator.hasNext()) {
            TopicPartition next = iterator.next();
            List<ConsumerRecord<String, String>> records1 = records.records(next);
            long offset = records1.get(records1.size() - 1).offset();
            map.put(next, new OffsetAndMetadata(offset + 1));
        }
        return map;
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-app-test-consumer");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-app-test-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    private static Properties producerProps() {
        Properties props =  new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-app-test");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafka-app-test-producer-2");
        return props;
    }

}
