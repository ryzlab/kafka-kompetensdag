package se.ryz.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/*

 # Set necessary variable(s)
 CONFLUENT_DOCKER_IP=`docker-machine ip confluent`

# START ---------- First Topic, the input to the KTable ----------

 TOPIC_NAME=counter-topic

 # Delete Topic if it exists
 docker run \
 --net=host \
 --rm \
 confluentinc/cp-kafka:5.1.0 \
 kafka-topics --delete \
 --if-exists \
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

# END   ---------- First Topic, the input to the KTable ----------

# START ---------- Second Topic, the output from the KTable ----------

 TOPIC_NAME=stats-topic

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

# END   ---------- Second Topic, the output from the KTable ----------

 # Describe the Topic
 docker run \
 --net=host \
 --rm \
 confluentinc/cp-kafka:5.1.0 \
 kafka-topics --describe --topic $TOPIC_NAME --zookeeper $CONFLUENT_DOCKER_IP:32181

 */
public class KTables {

    @Test
    public void runProducer() throws ExecutionException, InterruptedException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(null);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt=0; ;cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("counter-topic", null, Integer.toString(cnt));
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() +": key=null, value=" + cnt + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(5000);
        }
    }

    @Test
    public void computeKTableStatistics() throws InterruptedException {
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration("application2", null);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> counterStream = builder.stream("counter-topic", Consumed.with(Serdes.String(), Serdes.String()));

        counterStream.groupBy((key, value) -> {
            System.out.println ("Key: '" + key + "', value: '" + value + "'");
            String toReturn = "" + Long.parseLong(value)%3;
            System.out.println ("To return: '" + toReturn + "'");
            return toReturn;
        })
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

    @Test
    public void consumeKTableResult() throws InterruptedException {

        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration("application1", null);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Long> counterStream = builder.stream("stats-topic", Consumed.with(Serdes.String(), Serdes.Long()));

        counterStream.foreach((key, value) -> System.out.println ("Key: '" + key + "', value: '" + value + "'"));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        for (; ; ) {
            Thread.sleep(1000);
        }
    }


}
