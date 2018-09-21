package kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaTransactionProducer {

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());

        producer.initTransactions();
        producer.beginTransaction();

        IntStream.range(0, 10).forEach(i -> {
            try {
                producer.send(new ProducerRecord<>("tests", "2", "Hello, World")).get();
            } catch (InterruptedException | ExecutionException e) {
                producer.abortTransaction();
            }
        });

        producer.commitTransaction();
        producer.close();
    }

    private static Properties producerProps() {
        Properties props =  new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-app-test");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafka-app-test-producer-1");
        return props;
    }

}
