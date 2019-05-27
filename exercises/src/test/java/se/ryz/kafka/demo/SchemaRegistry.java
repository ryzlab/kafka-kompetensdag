package se.ryz.kafka.demo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import se.ryz.kafka.demo.avro.zombieweapon.Axe;
import se.ryz.kafka.demo.avro.zombieweapon.Chainsaw;
import se.ryz.kafka.demo.avro.zombieweapon.ZombieWeapon;
import se.ryz.kafka.demo.util.Common;

import java.util.Properties;

/*
Create necessary Topics


 TOPIC_NAME=zombiefight-avro-topicname-strategy
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


 TOPIC_NAME=zombiefight-avro-recordname-strategy
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
     * 1. Run {@link SchemaRegistry#sendWithTopicNamingStrategy()}.
     * 2. List subjects in Shcema Registry and see that our Avro Object is stored there.
     *    curl localhost:8081/subjects
     * 3. Run {@link SchemaRegistry#sendIncompatibleTypeWithTopicNamingStrategy()} and verify that we can't send
     *    message with another type
     * 4. run {@link SchemaRegistry#sendWithRecordNameStrategy()}. It will send two messages of different type
     * 5. List subjects in Schema Registry again.
     *    curl localhost:8081/subjects
     *    You will se the three messages defined in Schema Registry
     *
     * We can retrieve subject versions
     *     curl localhost:8081/subjects/se.ryz.kafka.demo.avro.zombieweapon.ZombieWeapon/versions
     *
     * We can also retrieve schema definition. Examples:
     *     curl localhost:8081/subjects/se.ryz.kafka.demo.avro.zombieweapon.ZombieWeapon/versions/latest
     *     curl localhost:8081/subjects/zombiefight-avro-topicname-strategy-value/versions/latest
     *
     * Extra work:
     * Modify the avro schema definition and re-run this exercise to try out compatible schema changes
     */


    /**
     * Create a KafkaProducer that sends a message of type '<'String, {@link ZombieWeapon}'>' to the topic
     * 'zombiefight-avro-topicname-strategy'. Use default naming strategy, just don't configure the producer property
     * {@link AbstractKafkaAvroSerDeConfig#VALUE_SUBJECT_NAME_STRATEGY}.
     */
    @Test
    public void sendWithTopicNamingStrategy() {
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
        ProducerRecord<String, ZombieWeapon> record = new ProducerRecord<>("zombiefight-avro-topicname-strategy", "fight1", zombieWeapon);
        producer.send(record);
        producer.flush();
        producer.close();
    }

    /**
     * Create a KafkaProducer that sends a message of type '<'String, {@link ZombieWeapon}'>' to the topic
     * 'zombiefight-avro-topicname-strategy'. Use default naming strategy, just don't configure the producer property
     * {@link AbstractKafkaAvroSerDeConfig#VALUE_SUBJECT_NAME_STRATEGY}.
     */
    @Test
    public void sendIncompatibleTypeWithTopicNamingStrategy() {
        Common common = new Common();
        Properties props = common.createProcessorProducerProperties(null);
        // Override Value Serializer to be Avro
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);
        // Default Key and Value name strategy is TopicNameStrategy

        KafkaProducer<String, Axe> producer = new KafkaProducer<>(props);
        Axe axe = new Axe(12);
        ProducerRecord<String, Axe> record = new ProducerRecord<>("zombiefight-avro-topicname-strategy", "fight1", axe);
        producer.send(record);
        producer.flush();
        producer.close();
    }


    /**
     * Now create a similar KafkaProducer as {@link SchemaRegistry#sendWithTopicNamingStrategy()} but now with the property
     * {@link AbstractKafkaAvroSerDeConfig#VALUE_SUBJECT_NAME_STRATEGY} set to
     * {@link io.confluent.kafka.serializers.subject.RecordNameStrategy} as the Schema naming Strategy.
     * Again, send a message of type '<'String, {@link ZombieWeapon}'>'
     */
    @Test
    public void sendWithRecordNameStrategy() {
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
        ProducerRecord<String, ZombieWeapon> record = new ProducerRecord<>("zombiefight-avro-recordname-strategy", "fight2", zombieWeapon);

        producer.send(record);
        producer.flush();
        producer.close();
        System.out.println ("Sent a ZombieWeapon");

        // Now we can happily send a message of another type
        KafkaProducer<String, Axe> axeProducer = new KafkaProducer<>(props);
        Axe axe = new Axe(12);
        ProducerRecord<String, Axe> axeRecord = new ProducerRecord<>("zombiefight-avro-topicname-strategy", "fight1", axe);
        axeProducer.send(axeRecord);
        axeProducer.flush();
        axeProducer.close();
        System.out.println("Successfully sent an Axe");
    }


}
