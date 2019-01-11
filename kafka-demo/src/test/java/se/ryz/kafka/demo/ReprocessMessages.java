package se.ryz.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/*
Two methods to reprocess, start consumer with new ID or reset partition offsets

Create a Topic with two partitions

 # Set necessary variable(s)
 CONFLUENT_DOCKER_IP=`docker-machine ip confluent`

 TOPIC_NAME=reprocess-messages-topic

  # Delete Topic if it exists
 docker run \
 --net=host \
 --rm \
 confluentinc/cp-kafka:5.1.0 \
 kafka-topics --delete \
 --if-exists \
 --topic $TOPIC_NAME \
 --zookeeper $CONFLUENT_DOCKER_IP:32181

 # Create Topic
 PARTITION_COUNT=3
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
public class ReprocessMessages {

    private static final String TOPIC_NAME = "reprocess-messages-topic";

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
        common.runProducerWithNullKey(TOPIC_NAME, "reprocessMessagesProducer");
    }


    /**
     * Run the consumer once and see that it consumes messages. Restart and see that
     * messages are consumed from where the previous run stopped processing.
     * We will not reprocess anything.
     * @throws InterruptedException
     */
    @Test
    public void runConsumerWithSameGroupId() throws InterruptedException {
        Common common = new Common();
        common.runSubscriptionConsumer(TOPIC_NAME, "sameGroupIdBetweenRuns", "rebalancingConsumerId");
    }
    /**
     * Run the consumer and see that it consumes messages from the beginning every time since client id always changes.
     * @throws InterruptedException
     */
    @Test
    public void runConsumerWithUUIDGroupId() throws InterruptedException {
        Common common = new Common();
        common.runSubscriptionConsumer(TOPIC_NAME, UUID.randomUUID().toString(), "client1");
    }

    /**
     * Run multiple times and see that we successfully seek to the beginning event if we have the
     * same group ID between runs.
     */
    @Test
    public void resetOffsetsAndRunConsumer() {
        Common common = new Common();
        String groupId = "sameGroupidBetweenRunsButResetOffsets";
        String clientId = null ;
        Properties consumerConfig = common.createConsumerConfig(groupId, clientId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);

        // Retrieve information about the topic partitions
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(TOPIC_NAME);
        // Create a list of TopicPartition objects so that we can assign them to the consumer
        List<TopicPartition> partitions = partitionInfoList.stream()
                .map(partitionInfo -> new TopicPartition(TOPIC_NAME, partitionInfo.partition()))
                .collect(Collectors.toList());

        // Print offsets
        partitions.forEach(partition -> System.out.println (TOPIC_NAME + ", partition: " + partition.partition() + ", committed: " + consumer.committed(partition) /*+ ", position: " + consumer..position(partition)*/));

        // Assign consumer the list of created TopicPartitions
        consumer.assign(partitions);
        // Tell the consumer to seek to beginning for all its partitions
        consumer.seekToEnd(partitions);


        for (;;) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                System.out.println("No records received") ;
            } else {
                for (Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator(); it.hasNext();) {
                    ConsumerRecord<String, String> record = it.next();
                    System.out.println ("   " + record.topic() + ", partition: " + record.partition() + ", key: " + record.key() + ", value: " + record.value());
                }
            }

        }
    }


}
