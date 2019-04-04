package se.ryz.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Convenience when running examples. Use this class to create Consumer/Producer properties etc.
 */
public class Common {
    /** Use this constant to get a reference to the Kafka Cluster */
    public static final String KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094" ;
    /** Use this constant to get a reference to the Schema Registry */
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    /** Use to connect to ZooKeeper ensemble */
    public static final String ZOOKEEPERS = "localhost:2181,localhost:2182,localhost:2183";
    /** Points to next label to return by {@link Common#getNextLabel()} */
    private int labelIndex;

    /** Labels that can be used when sending messages. Use convenience methods {@link Common#getNextLabel()} and {@link Common#getRandomLabel(int)} */
    private static String[] LABELS = {
            "Zombieland",
            "mad zombie disease",
            "Columbus",
            "Tallahassee",
            "Twinkies",
            "Wichita",
            "Little Rock",
            "Pacific Playland",
            "Cardio",
            "Double tap",
            "Beware of bathrooms",
            "Seatbelts",
            "Limber up",
            "Enjoy the little things",
            "Shaun",
            "Philip",
            "Liz",
            "Ed",
            "The Winchester",
            "Zombie Apocalypse",
            "Barbara",
            "Shaun of the Dead",
            "Dawn of the Dead",
            "WGON TV",
            "Nazi zombies",
            "Dead Snow",
            "Herzog",
            "Martin",
            "Roy",
            "Hanna",
            "Vegard",
            "Liv",
            "What We Do in the Shadows",
            "Viago",
            "Vladislav",
            "Deacon",
            "Petyr",
            "Procession of Shame",
            "We're Werewolves, not Swear-Wolves",
            "T-Virus",
            "Umbrella Corporation",
            "Rain",
            "Alice",
            "The Hive",
            "Raccoon City",
            "Red Queen"
    };

    /**
     * Will return a random label among the setSize first labels in the array {@link Common#LABELS}.
     * Useful to continously send messages but only use a certain number of
     * labels.
     * @param setSize Number of labels to consider when returning one
     * @return A random label among the setSize first labels in {@link Common#LABELS}.
     */
    public String getRandomLabel(int setSize) {
        return LABELS[(int)(Math.random()*setSize) % LABELS.length];
    }

    public Common() {
        labelIndex = 0;

    }

    /**
     * Loops continously over {@link Common#LABELS} and returns the next one
     * @return A label in {@link Common#LABELS}
     */
    public String getNextLabel() {
        labelIndex %= LABELS.length;
        return LABELS[labelIndex++];
    }

    /**
     * Convenience method for creating Kafka Consumer properties to use with KafkaConsumer
     * @param groupId Necessary Consumer Group ID. Cannot be null
     * @param clientId Optional Client ID, the client ID can be used to correlate events in Kafka with a certain client.
     * @return Created Properties object with some defaults.
     */
    public Properties createConsumerConfig(String groupId, String clientId) {
        final Properties props = new Properties();
        // Where to find Kafka broker(s).
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run. A new Application ID -> consume from beginning
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Client ID:
        // An optional identifier of a Kafka consumer (in a consumer group) that is passed to a Kafka broker with every request.
        // The sole purpose of this is to be able to track the source of requests beyond just ip and port by allowing a logical
        // application name to be included in Kafka logs and monitoring aggregates.
        if (clientId != null) {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        // Specify (de)serializers for record keys values.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        // Set MAX_POLL_INTERVAL_MS_CONFIG when poll will block for a long time. Otherwise the consumer
        // will think that the broker is down
        //props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * Convenience method for creating Kafka Consumer properties to use with Kafka Streams Client
     * @param applicationId Necessary Application ID used to group consumers
     * @param clientId An optional Identifier that can be used to correlate consumer activity in Kafka.
     * @return A Properties object with defaults.
     */
    public Properties createStreamsClientConfiguration(String applicationId, String clientId) {
        final Properties props = new Properties();
        // Where to find Kafka broker(s).
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run. A new Application ID -> consume from beginning
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

        // Client ID:
        // An optional identifier of a Kafka consumer (in a consumer group) that is passed to a Kafka broker with every request.
        // The sole purpose of this is to be able to track the source of requests beyond just ip and port by allowing a logical
        // application name to be included in Kafka logs and monitoring aggregates.
        if (clientId != null) {
            props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        }
        // Specify (de)serializers for record keys values.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        // If we do not commit and restart, we will get messages since last commit again when we restart
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");
        return props;
    }

    /**
     * sets up a Processor producer
     *      Processor Client ID
     *      Kafka Bootstrap servers
     *      Key and Value SerDes (both as String)
     * @param clientId
     * @return
     */
    public Properties createProcessorProducerProperties(String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // Used to trace activity in Kafka logs
        if (clientId != null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * Run the producer once to produce records with null key.
     * This will use round-robin scheme when producing messages to partitions. Each partition
     * will get every other message
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void runProducerWithNullKey(String topicName, String clientId) throws InterruptedException, ExecutionException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(clientId);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt=0; ;cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, Integer.toString(cnt));
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() +": key=" + record.key() + ", value=" + record.value() + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());

            Thread.sleep(2000);
        }
        //producer.close();
    }

    /**
     * Uses Kafka Processor API to continously consume {@link ConsumerRecords}.
     * @param topicName Topic to consume from. The Topic has to be created before calling this method
     * @param groupId The Consumer Group ID for this consumer.
     * @param clientId Optional Client ID
     */
    public void runSubscriptionConsumer(String topicName, String groupId, String clientId) {
        Common common = new Common();
        Properties props = common.createConsumerConfig(groupId, clientId);
        // Create the consumer and subscribe
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        // Poll for records and process them
        for (;;) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                System.out.println("No records received") ;
            } else {
                for (Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator(); it.hasNext();) {
                    ConsumerRecord<String, String> record = it.next();
                    System.out.println ("   " + record.topic() + ", partition: " + record.partition() +
                            ", key: " + record.key() + ", value: " + record.value());
                }
            }
        }
    }
}
