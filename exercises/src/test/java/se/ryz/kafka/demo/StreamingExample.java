package se.ryz.kafka.demo;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
/*
 Create a Topic with two partitions

 TOPIC_NAME=streaming-example-topic

 # Delete Topic if it exists
 kafka-topics --delete \
 --if-exists \
 --topic $TOPIC_NAME \
 --zookeeper localhost:2181,localhost:2182,localhost:2183


 # Create Topic
 PARTITION_COUNT=2
 REPLICATION_FACTOR=1
 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:2181,localhost:2182,localhost:2183

 # Describe the Topic
 kafka-topics --describe --topic $TOPIC_NAME --zookeeper localhost:2181,localhost:2182,localhost:2183

 */

public class StreamingExample {

    private static final String TOPIC_NAME = "streaming-example-topic";

    /**
     * In this lab we will test a simple KStream producer and consumer.
     * 1. Start the consumer {@link StreamingExample#AconsumeMessagesAsStream()}
     * 2. Start the producer {@link StreamingExample#BproduceMessages()} and see that the messages comes through
     * 3. Start the consumer {@link StreamingExample#CconsumeFromNonexistingTopic()} that consumes from an unknown Topic.
     *      What happens and why?
     */

    /**
     * Consumes messages of the type '<'String, String'>' from the topic TOPIC_NAME and prints the messages to the console.
     * @throws InterruptedException
     */
    @Test
    public void AconsumeMessagesAsStream() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration(this.getClass().getName() + "-application", this.getClass().getName() + "-client");
        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> messageStream = builder.stream(TOPIC_NAME);
        messageStream.foreach((key, value) -> System.out.println("Key: " + key + ", value: " + value));

        // Create and start the stream
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        for (; ; ) {
            Thread.sleep(1000);
        }
    }

    /**
     * Write a producer that sends messages of the type '<'String, String'>' to the topic TOPIC_NAME.
     * Tip: To send messages with a simple value, use {@link Faker}
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void BproduceMessages() throws ExecutionException, InterruptedException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(this.getClass().getName() + "-producer-client");
        Faker faker = new Faker();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (;;) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, faker.lebowski().quote());
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() + ": , offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(2000);
        }
    }


    /**
     * Write a KSTream consumer that consumes from a topic not created. What happens and why?
     * @throws InterruptedException
     */
    @Test
    public void CconsumeFromNonexistingTopic() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration(this.getClass().getName() + "-application", this.getClass().getName() + "-client");
        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> producedMessages = builder.stream("THIS-TOPIC-DOES-NOT-EXIST");
        producedMessages.foreach((key, value) -> System.out.println("Key: " + key + ", value: " + value));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        for (; ; ) {
            Thread.sleep(1000);
        }
    }

}
