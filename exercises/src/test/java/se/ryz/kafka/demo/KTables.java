package se.ryz.kafka.demo;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;
import se.ryz.kafka.demo.util.Common;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/*

Open up a shell and remember to set PATH to .../confluent-x.y.z/bin


# Create the first topic

 TOPIC_NAME=counter

 # Delete Topic if it exists
    kafka-topics --delete \
        --if-exists \
        --topic $TOPIC_NAME \
        --zookeeper localhost:2181,localhost:2182,localhost:2183

 # Create Topic(s) for Lab
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


# Create the second topic

 TOPIC_NAME=stats

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
public class KTables {


    /**
     * Here, we will create a producer, an intermediate aggregate processor, and a final consumer
     * that prints the result to the console.
     */


    /**
     * Write a Producer that produces messages of type '<'String, String'>' to the topic 'counter'.
     * Set the message value by using {@link Faker}.
     * Produce messages with a sleep in between to keep things at a sane speed while doing the lab.
     * The content of the key is not important for the lab so just set it to null.
     */
    @Test
    public void runProducer() throws ExecutionException, InterruptedException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(null);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt = 0; ; cnt++) {
            Faker faker = new Faker(new Random((int) (Math.random() * 6)));
            String value = faker.ancient().god();
            String key = null ;
            ProducerRecord<String, String> record = new ProducerRecord<>("counter", key, value);
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            producer.flush();
            System.out.println(recordMetadata.topic() + ": key=" + key + ", value=" + value + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(1000);
        }
    }

    /**
     * Create a KStream consuming from 'counter', does some computing of the messages, and writes the result
     * to the stream 'stats'
     * Do a count of all individual message keys
     */
    @Test
    public void computeKTableStatistics() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration("application2", null);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> counterStream = builder.stream("counter", Consumed.with(Serdes.String(), Serdes.String()));

        counterStream.groupBy((key, value) -> value)
                .count()
                .toStream()
                .to("stats", Produced.with(Serdes.String(), Serdes.Long()));
        streamsConfiguration.put("commit.interval.ms", 500);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        for (; ; ) {
            Thread.sleep(1000);
        }

    }

    /**
     * Creates a consumer that consumes messages of type '<'String, Long'>' from 'stats' and print them to the console.
     * You should get the count of all individual keys sent from {@link KTables#computeKTableStatistics()}
     * @throws InterruptedException
     */
    @Test
    public void consumeKTableResult() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration("application1", null);
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Long> counterStream = builder.stream("stats", Consumed.with(Serdes.String(), Serdes.Long()));
        counterStream.foreach((key, value) -> System.out.println("Key: '" + key + "', value: '" + value + "'"));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        for (; ; ) {
            Thread.sleep(1000);
        }
    }


}
