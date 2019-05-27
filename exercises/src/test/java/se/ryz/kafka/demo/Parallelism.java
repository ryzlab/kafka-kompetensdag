package se.ryz.kafka.demo;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import se.ryz.kafka.demo.util.Common;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/*

Open up a shell and remember to set PATH to .../confluent-x.y.z/bin

We will be using one Topic

TOPIC_NAME=consumer-rebalance

# Delete Topic if it exists
kafka-topics --delete \
--if-exists \
--topic $TOPIC_NAME \
--zookeeper localhost:2181,localhost:2182,localhost:2183

# Create Topic
PARTITION_COUNT=2
REPLICATION_FACTOR=2

kafka-topics --create \
--topic $TOPIC_NAME \
--partitions $PARTITION_COUNT \
--replication-factor $REPLICATION_FACTOR \
--if-not-exists \
--config min.insync.replicas=2 \
--zookeeper localhost:2181,localhost:2182,localhost:2183

# To verify that the creation of the topic was successful:
kafka-topics --describe --topic $TOPIC_NAME --zookeeper localhost:2181,localhost:2182,localhost:2183

 */
public class Parallelism {

    private static final String TOPIC_NAME = "consumer-rebalance";

    /**
     * Lab instructions:
     * We will test consumer rebalancing that occurs when clients are added/removed from a Kafka Consumer Group.
     * We will test message distribution that depends on the message key and see how Kafka distributes messages
     * to consumers.
     * 1. We have 2 partitions, so run {@link Parallelism#runConsumerWithSubscription()} twice, each in its own tab and keep them running
     * 2. Run {@link Parallelism#runProducerWithSameKey()}. What happens and why?
     * 3. Stop {@link Parallelism#runProducerWithSameKey()}
     * 4. Start {@link Parallelism#runProducerWithNullKey()}. What happens and why?
     * 5. Start a third {@link Parallelism#runConsumerWithSubscription()}. What happens and why? (Hint: How many partitions do we have?)
     */

    /**
     * Create a consumer that consumes from the topic TOPIC_NAME and prints messages to the console
     * @throws InterruptedException
     */
    @Test
    public void runConsumerWithSubscription() throws InterruptedException {
        Common common = new Common();
        common.runSubscriptionConsumer(TOPIC_NAME, "consumerRebalancingGroup", "rebalancingConsumerId");
        /*Properties consumerConfig = common.createConsumerConfig("consumerRebalancingGroup", "client1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        for (;;) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(100));
            if (consumerRecords.isEmpty()) {
                System.out.println("No records received") ;
            } else {
                for (Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator(); it.hasNext();) {
                    ConsumerRecord<String, String> record = it.next();
                    System.out.println ("   " + record.topic() + ", partition: " + record.partition() + ", key: " + record.key() + ", value: " + record.value());
                }
            }

        }*/
    }

    /**
     * Write a KafkaProducer that sends messages to TOPIC_NAME with the same key. Sleep 2 seconds between messages.
     * Tip: Use Faker to produce messages with content
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void BrunProducerWithSameKey() throws InterruptedException, ExecutionException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties("consumerRebalancingProducer");
        Faker faker = new Faker();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt=0; ;cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "0", faker.rickAndMorty().character());
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();

            System.out.println(recordMetadata.topic() +": key=" + record.key() + ", value=" + record.value() + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(2000);
        }
    }

    /**
     * Write a KafkaProducer that sends messages to TOPIC_NAME with a null key. Sleep 2 seconds between messages.
     * Tip: Use {@link Faker} to produce the message value.
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void runProducerWithNullKey() throws InterruptedException, ExecutionException {
        Common common = new Common();
        common.runProducerWithNullKey(TOPIC_NAME, "consumerRebalancingProducer");
    }


}
