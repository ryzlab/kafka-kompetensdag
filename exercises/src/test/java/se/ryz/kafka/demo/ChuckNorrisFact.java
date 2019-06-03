package se.ryz.kafka.demo;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;
import se.ryz.kafka.demo.util.Common;

import java.util.Properties;

/*


Open up a shell and remember to set PATH to .../confluent-x.y.z/bin


# Create the first topic

 TOPIC_NAME=chuck-norris-fact

 # Delete Topic if it exists
    kafka-topics --delete \
        --if-exists \
        --topic $TOPIC_NAME \
        --zookeeper localhost:2181,localhost:2182,localhost:2183

 # Create Topic(s) for Lab
 PARTITION_COUNT=1
 REPLICATION_FACTOR=3

 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:2181,localhost:2182,localhost:2183

# To verify that the creation of the topic was successful:
kafka-topics --describe --topic $TOPIC_NAME --zookeeper localhost:2181,localhost:2182,localhost:2183

# Create the topic where the facts are shouted!

 TOPIC_NAME=chuck-norris-shout-fact

 # Delete Topic if it exists
    kafka-topics --delete \
        --if-exists \
        --topic $TOPIC_NAME \
        --zookeeper localhost:2181,localhost:2182,localhost:2183

 # Create Topic(s) for Lab
 PARTITION_COUNT=1
 REPLICATION_FACTOR=3

 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:2181,localhost:2182,localhost:2183

# To verify that the creation of the topic was successful:
kafka-topics --describe --topic $TOPIC_NAME --zookeeper localhost:2181,localhost:2182,localhost:2183


# Start KSQL shell and print the Topic content
$ ksql

# Bonus: Go into kafka-manager ant see that the Topic 'chuck-norris-shout-fact' exists and note which broker is the leader.
# Kill the leader broker.

docker kill KAFKA_HOST

# Start it again. Look in kafka-manager and see which broker is the leader and whether it is preferred.
# Trigger a rebalance


 */
public class ChuckNorrisFact {

    /**
     * Connects to the Kafka Cluster, creates a Producer and
     * produces facts about Chuck Norris at a timely interval
     * @throws InterruptedException Don't really think so...
     */
    @Test
    public void chuckNorrisFactProducer() throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "chuck-norris-fact-producer-v1.0");
        KafkaProducer<String, String> quoteProducer = new KafkaProducer<>(props);
        Faker faker = new Faker();
        for (int cnt=0; ; cnt++) {
            String key = "Chuck Norris Fact # " + cnt;
            String fact = faker.chuckNorris().fact();
            System.out.println("Sending fact " + fact);
            ProducerRecord<String, String> movieQuoteRecord = new ProducerRecord<>("chuck-norris-fact", key, fact);
            quoteProducer.send(movieQuoteRecord);
            quoteProducer.flush();
            Thread.sleep(2000);
        }
    }

    @Test
    public void chuckNorrisFactShouter() throws InterruptedException {
        final Properties streamsConfiguration = new Properties();
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_BROKERS);
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run. A new Application ID -> consume from beginning
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "chuck-norris-shouter-v1.0");

        // Specify (de)serializers for record keys values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        // If we do not commit and restart, we will get messages since last commit again when we restart
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> messageStream = builder.stream("chuck-norris-fact");
        messageStream
                .map((quoteNumber, quote) -> new KeyValue(quoteNumber, quote.toUpperCase()))
                .to("chuck-norris-shout-fact");

        // Create and start the stream
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        for (; ; ) {
            Thread.sleep(1000);
        }

    }

}
