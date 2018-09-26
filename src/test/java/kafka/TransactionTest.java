package kafka;

import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

import io.vavr.Tuple;
import kafka.security.auth.Topic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import scala.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionTest {

    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    @Test
    public void allMessageShouldBePublished() {
        String methodName = "allMessageShouldBePublished";

        getKafkaTestUtils()
            .createTopic(methodName, 1, (short) 1);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps(methodName));
        producer.initTransactions();
        producer.beginTransaction();

        IntStream.range(0, 10).forEach(i -> {
            producer.send(new ProducerRecord<>(methodName, 1, "Hello, World!"));
        });
        producer.commitTransaction();

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps(methodName));
        consumer.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(5000));
        consumer.close();

        assertEquals(10, records.count());
    }

    @Test
    public void noMessageShouldBePublished() {
        String methodName = "noMessageShouldBePublished";

        getKafkaTestUtils()
            .createTopic(methodName, 1, (short) 1);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps(methodName));
        producer.initTransactions();
        producer.beginTransaction();

        try {
            IntStream.range(0, 10).forEach(i -> {
                if (i == 9) {
                    throw new RuntimeException();
                }
                producer.send(new ProducerRecord<>(methodName, 1, "Hello, World!"));
            });
            producer.commitTransaction();
        } catch (RuntimeException e) {
            producer.abortTransaction();
        }

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps(methodName));
        consumer.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(5000));
        consumer.close();

        assertTrue(records.isEmpty());
    }

    @Test
    public void aConsumerShouldCommitItsOffsets() {
        String methodName = "aConsumerShouldCommitItsOffsets";

        getKafkaTestUtils()
            .createTopic(methodName, 1, (short) 1);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps(methodName));
        producer.initTransactions();
        producer.beginTransaction();

        IntStream.range(0, 10).forEach(i -> {
            producer.send(new ProducerRecord<>(methodName, 1, "Hello, World!"));
        });
        producer.commitTransaction();

        KafkaConsumer<Integer, String> consumer1 = new KafkaConsumer<>(consumerProps(methodName));
        consumer1.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC1 = consumer1.poll(Duration.ofMillis(5000));
        consumer1.close();

        KafkaConsumer<Integer, String> consumer2 = new KafkaConsumer<>(consumerProps(methodName));
        consumer2.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC2 = consumer2.poll(Duration.ofMillis(5000));
        consumer2.close();

        assertEquals(10, recordsC1.count());
        assertEquals(10, recordsC2.count());
    }

    @Test
    public void committedOffsetShouldNotBeReadAgain() {
        String methodName = "committedOffsetShouldNotBeReadAgain";

        getKafkaTestUtils()
            .createTopic(methodName, 1, (short) 1);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps(methodName));
        producer.initTransactions();
        producer.beginTransaction();

        IntStream.range(0, 10).forEach(i -> {
            producer.send(new ProducerRecord<>(methodName, 1, "Hello, World!"));
        });
        producer.commitTransaction();

        KafkaConsumer<Integer, String> consumer1 = new KafkaConsumer<>(consumerProps(methodName));
        consumer1.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC1 = consumer1.poll(Duration.ofMillis(5000));

        producer.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = consumerOffsets(recordsC1);
        producer.sendOffsetsToTransaction(offsets, methodName);
        producer.commitTransaction();

        consumer1.close();

        KafkaConsumer<Integer, String> consumer2 = new KafkaConsumer<>(consumerProps(methodName));
        consumer2.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC2 = consumer2.poll(Duration.ofMillis(5000));

        assertEquals(10, recordsC1.count());
        assertEquals(0, recordsC2.count());
    }

    @Test
    public void committingAllOffsetOrTheLastOneIsTheSame() {
        String methodName = "committingAllOffsetOrTheLastOneIsTheSame";

        getKafkaTestUtils()
                .createTopic(methodName, 1, (short) 1);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps(methodName));
        producer.initTransactions();
        producer.beginTransaction();

        IntStream.range(0, 10).forEach(i -> {
            producer.send(new ProducerRecord<>(methodName, 1, "Hello, World!"));
        });
        producer.commitTransaction();

        KafkaConsumer<Integer, String> consumer1 = new KafkaConsumer<>(consumerProps(methodName));
        consumer1.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC1 = consumer1.poll(Duration.ofMillis(5000));

        producer.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = lastConsumerOffsets(recordsC1);
        producer.sendOffsetsToTransaction(offsets, methodName);
        producer.commitTransaction();

        consumer1.close();

        KafkaConsumer<Integer, String> consumer2 = new KafkaConsumer<>(consumerProps(methodName));
        consumer2.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC2 = consumer2.poll(Duration.ofMillis(5000));

        assertEquals(10, recordsC1.count());
        assertEquals(0, recordsC2.count());
    }

    @Test
    public void transactionsAreOnTheLogOffsetNotPerMessage() {
        String methodName = "transactionsAreOnTheLogOffsetNotPerMessage";

        getKafkaTestUtils()
                .createTopic(methodName, 1, (short) 1);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps(methodName));
        producer.initTransactions();
        producer.beginTransaction();

        IntStream.range(0, 2).forEach(i -> {
            producer.send(new ProducerRecord<>(methodName, 1, String.valueOf(i)));
        });
        producer.commitTransaction();

        // The first one fails
        KafkaConsumer<Integer, String> consumer1 = new KafkaConsumer<>(consumerProps(methodName));
        consumer1.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC1 = consumer1.poll(Duration.ofMillis(5000));

        assertEquals(2, recordsC1.count());

        Iterator<ConsumerRecord<Integer, String>> iterator = recordsC1.iterator();
        iterator.next();

        producer.beginTransaction();

        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        ConsumerRecord<Integer, String> secondRecord = iterator.next();
        map.put(new TopicPartition(methodName, secondRecord.partition()), new OffsetAndMetadata(secondRecord.offset() + 1));

        producer.sendOffsetsToTransaction(map, methodName);
        producer.commitTransaction();

        consumer1.close();

        KafkaConsumer<Integer, String> consumer2 = new KafkaConsumer<>(consumerProps(methodName));
        consumer2.subscribe(Collections.singleton(methodName));
        ConsumerRecords<Integer, String> recordsC2 = consumer2.poll(Duration.ofMillis(5000));

        assertEquals(0, recordsC2.count());
    }

    private KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }

    private Properties producerProps(String transactionId) {
        Properties producerProps = new Properties();
         producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return producerProps;
    }

    private static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    private static Map<TopicPartition, OffsetAndMetadata> consumerOffsets(ConsumerRecords<Integer, String> records) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        records.iterator().forEachRemaining(r -> {
            map.put(
                    new TopicPartition(r.topic(), r.partition()),
                    new OffsetAndMetadata(r.offset() + 1)
            );
        });
        return map;
    }

    private static Map<TopicPartition, OffsetAndMetadata> lastConsumerOffsets(ConsumerRecords<Integer, String> records) {
        List<Tuple2<TopicPartition, OffsetAndMetadata>> list = new ArrayList<>();
        records.iterator().forEachRemaining(r -> {
            list.add(new Tuple2<>(
                    new TopicPartition(r.topic(), r.partition()),
                    new OffsetAndMetadata(r.offset() + 1)
            ));
        });

        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        Tuple2<TopicPartition, OffsetAndMetadata> lastItem = list.get(list.size() - 1);
        map.put(lastItem._1, lastItem._2);

        return map;
    }

}
