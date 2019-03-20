package se.ryz.kafka.demo;

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
 * Create a Topic with two partitions

 TOPIC_NAME=streaming-example-topic

 # Delete Topic if it exists
 kafka-topics --delete \
 --if-exists \
 --topic $TOPIC_NAME \
 --zookeeper localhost:22181,localhost:32181,localhost:42181


 # Create Topic
 PARTITION_COUNT=2
 REPLICATION_FACTOR=1
 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:22181,localhost:32181,localhost:42181

 # Describe the Topic
 kafka-topics --describe --topic $TOPIC_NAME --zookeeper localhost:22181,localhost:32181,localhost:42181

 */

public class StreamingExample {

    private static final String TOPIC_NAME = "streaming-example-topic";

    @Test
    public void produceMessages() throws ExecutionException, InterruptedException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(this.getClass().getName() + "-producer-client");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt = 0; ; cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, Integer.toString(cnt));
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() + ": key=null, value=" + cnt + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(2000);
        }
    }

    @Test
    public void consumeFromNonexistingTopic() throws InterruptedException {
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

    @Test
    public void consumeMessagesAsStream() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration(this.getClass().getName() + "-application", this.getClass().getName() + "-client");
        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> producedMessages = builder.stream(TOPIC_NAME);
        producedMessages.foreach((key, value) -> System.out.println("Key: " + key + ", value: " + value));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        for (; ; ) {
            Thread.sleep(1000);
        }
    }
}
