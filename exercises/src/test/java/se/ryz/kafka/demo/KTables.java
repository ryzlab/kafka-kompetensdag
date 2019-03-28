package se.ryz.kafka.demo;

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

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/*

Open up a shell and remember to set PATH to .../confluent-x.y.z/bin


# Create the first topic

# START ---------- First Topic, the input to the KTable ----------

 TOPIC_NAME=counter-topic

 # Delete Topic if it exists
    kafka-topics --delete \
        --if-exists \
        --topic $TOPIC_NAME \
        --zookeeper localhost:22181,localhost:32181,localhost:42181

 # Create Topic(s) for Lab
 PARTITION_COUNT=2
 REPLICATION_FACTOR=2


 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:22181,localhost:32181,localhost:42181

# END   ---------- First Topic, the input to the KTable ----------

# Create the second topic
# START ---------- Second Topic, the output from the KTable ----------

 TOPIC_NAME=stats-topic

 # Delete Topic if it exists
 kafka-topics --delete \
 --if-exists \
 --topic $TOPIC_NAME \
 --zookeeper $CONFLUENT_DOCKER_IP:32181

 # Create Topic
 PARTITION_COUNT=2
 REPLICATION_FACTOR=2

 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:22181,localhost:32181,localhost:42181

# END   ---------- Second Topic, the output from the KTable ----------

# To verify that the creation of the topic was successful:
kafka-topics --describe --topic $TOPIC_NAME --zookeeper $CONFLUENT_DOCKER_IP:32181

 */
public class KTables {


    /**
     * Here, we will create a producer, an intermediate aggregate processor, and a final consumer
     * that prints the result to the console.
     */


    /**
     * Write a Producer that produces messages of type '<'String, String'>' to the topic 'counter-topic'.
     * Set the message value by using {@link Common#getRandomLabel(int)} with a value of 6.
     * Produce messages with a sleep in between to keep things at a sane speed while doing the lab.
     * The content of the key is not important for the lab so just set it to null.
     */
    @Test
    public void ArunProducer() throws ExecutionException, InterruptedException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(null);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt = 0; ; cnt++) {
            String value = common.getRandomLabel(6);
            String key = null ;
            ProducerRecord<String, String> record = new ProducerRecord<>("counter-topic", key, value);
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() + ": key=" + key + ", value=" + value + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(5000);
        }
    }

    /**
     * Create a KStream consuming from 'counter-topic', does some computing of the messages, and writes the result
     * to the stream 'stats-topic'
     * Do a count of all individual message keys
     */
    @Test
    public void BcomputeKTableStatistics() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration("application2", null);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> counterStream = builder.stream("counter-topic", Consumed.with(Serdes.String(), Serdes.String()));

        counterStream.groupBy((key, value) -> value)
                .count()
                .toStream()
                .to("stats-topic", Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        for (; ; ) {
            Thread.sleep(1000);
        }

    }

    /**
     * Write a consumer that consumes messages of type '<'String, Long'>' from 'stats-topic' and print them to the console.
     * You should get the count of all individual keys sent from {@link KTables#BcomputeKTableStatistics()}
     * @throws InterruptedException
     */
    @Test
    public void CconsumeKTableResult() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration("application1", null);
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Long> counterStream = builder.stream("stats-topic", Consumed.with(Serdes.String(), Serdes.Long()));
        counterStream.foreach((key, value) -> System.out.println("Key: '" + key + "', value: '" + value + "'"));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        for (; ; ) {
            Thread.sleep(1000);
        }
    }


}
