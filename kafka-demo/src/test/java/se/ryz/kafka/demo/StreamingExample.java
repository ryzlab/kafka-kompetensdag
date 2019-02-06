package se.ryz.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
/*
 * Create a Topic with two partitions

 # Set necessary variable(s)
 CONFLUENT_DOCKER_IP=`docker-machine ip confluent`

 TOPIC_NAME=streaming-example-topic

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
 PARTITION_COUNT=2
 REPLICATION_FACTOR=1
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

public class StreamingExample {

    private static final String TOPIC_NAME = "streaming-example-topic";
    String kafkaDockerHost = "192.168.99.100";
    String kafkaBrokers =  kafkaDockerHost + ":29092," + kafkaDockerHost + ":39092," + kafkaDockerHost + ":49092";

    @Test
    public void produceMessages() throws ExecutionException, InterruptedException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(this.getClass().getName() + "-producer-client");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt=0; ;cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, Integer.toString(cnt));
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() +": key=null, value=" + cnt + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
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
