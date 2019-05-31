package se.ryz.kafka.demo.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static se.ryz.kafka.demo.util.Common.KAFKA_BROKERS;

/**
 * Contains some code that can be used to interact with a Kafka cluster. Use as inspiration!!
 */
public class DemoTest {

    /**
     * Loops over all Topics and prints some details about them
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void listTopicsDetails() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        AdminClient adminClient = AdminClient.create(props);

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        // Get a list of all Topics
        Collection<TopicListing> topicListings = adminClient.listTopics(listTopicsOptions).listings().get();
        System.out.println("Number of registered topics: " + topicListings.size());

        // Loop over each Topic
        for (Iterator<TopicListing> it = topicListings.iterator(); it.hasNext();) {
            TopicListing topicListing = it.next();
            System.out.println("\nTopic name: "  + topicListing.name());
            // Describe the topic we are looping over
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(new String[]{topicListing.name()} ));
            // We get the single future that we requested
            KafkaFuture<Map<String, TopicDescription>> futures = describeTopicsResult.all();
            // Retrieve Topic Description
            TopicDescription topicDescription = futures.get().get(topicListing.name());
            List<TopicPartitionInfo> partitionInfos = topicDescription.partitions();
            System.out.println ("    Number of partitions: " + partitionInfos.size());
            // Loop over the Topics Partitions and print info
            for (TopicPartitionInfo partitionInfo : partitionInfos)
            {
                System.out.println("    Partition: " + partitionInfo.partition());
                System.out.println("        Leader: " + partitionInfo.leader());
                System.out.println("        Number of ISRs: " + partitionInfo.isr().size());
                for (Node isrNode : partitionInfo.isr()) {
                    System.out.println("            ISR Node: " + isrNode);
                }
                System.out.println("        Number of replicas: " + partitionInfo.replicas().size());
                for (Node replica : partitionInfo.replicas()) {
                    System.out.println ("            Replica node: " + replica);
                }
            }
        }

    }

    /**
     * Prints registered topics to console
     */
    @Test
    public void listTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        AdminClient adminClient = AdminClient.create(props);
        //adminClient.describeCluster();

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(false);
        Collection<TopicListing> topicListings = adminClient.listTopics(listTopicsOptions).listings().get();
        System.out.println("Number of registered topics (without internal): " + topicListings.size());

        topicListings.forEach(topicListing -> System.out.println("Topic name: '" + topicListing.name() + "', is internal: " + topicListing.isInternal()));

        System.out.println();
        listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        topicListings = adminClient.listTopics(listTopicsOptions).listings().get();
        System.out.println("Number of registered topics (with internal): " + topicListings.size());
        topicListings.forEach(topicListing -> System.out.println("Topic name: '" + topicListing.name() + "', is internal: " + topicListing.isInternal()));
    }



    @Test
    public void viewClusterNodeInfo() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        AdminClient adminClient = AdminClient.create(props);
        DescribeClusterResult descibeClusterResult = adminClient.describeCluster();
        KafkaFuture<Collection<Node>> nodesFuture = descibeClusterResult.nodes();
        Collection<Node> nodes =nodesFuture.get();
        System.out.println ("Number of Nodes in the cluster: " + nodes.size());
        for (Node node : nodes) {
            System.out.println ("   Node: " + node);

        }
        System.out.println("Cluster Controller: " + descibeClusterResult.controller().get());
    }




}
