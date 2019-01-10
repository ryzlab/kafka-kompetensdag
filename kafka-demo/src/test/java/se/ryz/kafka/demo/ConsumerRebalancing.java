package se.ryz.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/*
 * Create a Topic with two partitions

 # Set necessary variable(s)
 CONFLUENT_DOCKER_IP=`docker-machine ip confluent`

 TOPIC_NAME=consumer-rebalance-topic

 # Delete old Topic
 docker run \
 --net=host \
 --rm \
 confluentinc/cp-kafka:5.1.0 \
 kafka-topics --delete \
 --topic $TOPIC_NAME \
 --zookeeper $CONFLUENT_DOCKER_IP:32181

 # Create Topic(s) for Lab
 PARTITION_COUNT=2
 REPLICATION_FACTOR=2


 docker run \
 --net=host \
 --rm \
 confluentinc/cp-kafka:5.1.0 \
 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper $CONFLUENT_DOCKER_IP:32181

 # Describe the Topic
 docker run \
 --net=host \
 --rm \
 confluentinc/cp-kafka:5.1.0 \
 kafka-topics --describe --topic $TOPIC_NAME --zookeeper $CONFLUENT_DOCKER_IP:32181

 */
public class ConsumerRebalancing {

    private static final String TOPIC_NAME = "consumer-rebalance-topic";

    /**
     * Run the producer once to produce records with null key.
     * This will use round-robin scheme when producing messages to partitions. Each partition
     * will get every other message
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void runProducerWithNullKey() throws InterruptedException, ExecutionException {
        Common common = new Common();
        common.runProducerWithNullKey(TOPIC_NAME, "consumerRebalancingProducer");
    }

    /**
     * Run the producer once to produce records with the same key. This will send every message to the same partition
     * and load balancing will not be effective.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void runProducerWithSameKey() throws InterruptedException, ExecutionException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties("consumerRebalancingProducer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt=0; ;cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "0", Integer.toString(cnt));
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() +": key=null, value=" + cnt + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(2000);
        }
        //producer.close();
    }

    /**
     * Run the consumer once and see that it consumes messages from both partitions.
     * Start another consumer and see that each of them will be assigned another partition. This
     * is because we have the same group.
     * Start a third and see that it does not get any messages.
     *
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
}
