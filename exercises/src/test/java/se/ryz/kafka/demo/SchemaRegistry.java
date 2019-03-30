package se.ryz.kafka.demo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;
import se.ryz.kafka.demo.avro.zombieweapon.Axe;
import se.ryz.kafka.demo.avro.zombieweapon.Chainsaw;
import se.ryz.kafka.demo.avro.zombieweapon.ZombieWeapon;

import java.util.Properties;

/*

 TOPIC_NAME=zombiefight-avro-topicname-topic
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


 TOPIC_NAME=zombiefight-avro-recordname-topic
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


 */
public class SchemaRegistry {

    /**
     * We will send messages of Avro type with different Schema naming strategy.
     * 0. List registered subjects in Schema Registry
     *    curl localhost:8081/subjects
     * 1. Run {@link SchemaRegistry#AsendWithDefaultNamingStrategy()}.
     * 2. List subjects in Shcema Registry and see that our Avro Object is stored there.
     *    curl localhost:8081/subjects
     * 3. run {@link SchemaRegistry#BsendWithRecordNameStrategy()}
     * 4. List subjects in Schema Registry again.
     *    curl localhost:8081/subjects
     *
     * Try to instantiate an Avro object of another type and send it to 'zombiefight-avro-topicname-topic' and
     * see what happens.
     *
     * We can retrieve subject versions
     *     curl localhost:8081/subjects/se.ryz.kafka.demo.avro.zombieweapon.ZombieWeapon/versions
     *
     * We can also retrieve schema definition. Examples:
     *     curl localhost:8081/subjects/se.ryz.kafka.demo.avro.zombieweapon.ZombieWeapon/versions/latest
     *     curl localhost:8081/subjects/zombiefight-avro-topicname-topic-value/versions/latest
     *
     * Extra work:
     * Modify the avro schema definition and re-run this exercise to try out compatible schema changes
     */


    /**
     * Create a KafkaProducer that sends a message of type '<'String, {@link ZombieWeapon}'>' to the topic
     * 'zombiefight-avro-topicname-topic'. Use default naming strategy, just don't configure the producer property
     * {@link AbstractKafkaAvroSerDeConfig#VALUE_SUBJECT_NAME_STRATEGY}.
     */
    @Test
    public void AsendWithDefaultNamingStrategy() {
        Common common = new Common();
        Properties props = common.createProcessorProducerProperties(null);
        // Override Value Serializer to be Avro
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);
        // Default Key and Value name strategy is TopicNameStrategy

        KafkaProducer<String, ZombieWeapon> producer = new KafkaProducer<>(props);
        Chainsaw command = new Chainsaw("ZCS5817, ZOMBI 16\" 58V 4AH CHAINSAW");
        ZombieWeapon zombieWeapon = new ZombieWeapon("Command description", command);
        ProducerRecord<String, ZombieWeapon> record = new ProducerRecord<>("zombiefight-avro-topicname-topic", "fight1", zombieWeapon);
        producer.send(record);
        producer.flush();
        producer.close();
    }

    /**
     * Now create a similar KafkaProducer as {@link SchemaRegistry#AsendWithDefaultNamingStrategy()} but now with the property
     * {@link AbstractKafkaAvroSerDeConfig#VALUE_SUBJECT_NAME_STRATEGY} set to
     * {@link io.confluent.kafka.serializers.subject.RecordNameStrategy} as the Schema naming Strategy.
     * Again, send a message of type '<'String, {@link ZombieWeapon}'>'
     */
    @Test
    public void BsendWithRecordNameStrategy() {
        Common common = new Common();
        Properties props = common.createProcessorProducerProperties(null);
        // Override Value Serializer to be Avro
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // Override the default Key and Value name strategy
        props.put(AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);
        // Override default naming strategy
        props.put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

        KafkaProducer<String, ZombieWeapon> producer = new KafkaProducer<>(props);
        Axe weapon = new Axe(8);
        ZombieWeapon zombieWeapon = new ZombieWeapon("Command description", weapon);
        ProducerRecord<String, ZombieWeapon> record = new ProducerRecord<>("zombiefight-avro-recordname-topic", "fight2", zombieWeapon);
        producer.send(record);
        producer.flush();
        producer.close();
    }

    /**
     * Write a KStream that receives messages from 'hello-world-avro' topic and prints messages to the console.
     * @throws InterruptedException
     */
/*    @Test
    public void Cconsume() throws InterruptedException {
        Common common = new Common();
        common.createConsumerConfig("consumer-" + this.getClass().getName(), "client-" + this.getClass().getName());
        Properties streamsConfiguration = common.createStreamsClientConfiguration(this.getClass().getName(), null);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, HelloWorldCommand> commandStream = builder.stream("hello-world-avro");
        commandStream.foreach((key, command) -> {
            System.out.println("Command received: " + command.getCommand());
            System.out.println("Request? " + (command.getCommand() instanceof HelloWorldRequest));
            System.out.println("Reply? " + (command.getCommand() instanceof HelloWorldReply));
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        for (; ; ) {
            Thread.sleep(1000);
        }
    }*/
}
