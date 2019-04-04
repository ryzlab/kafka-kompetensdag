package se.ryz.kafka.demo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;
import se.ryz.kafka.demo.avro.zombieweapon.Axe;
import se.ryz.kafka.demo.avro.zombieweapon.ZombieWeapon;

import java.util.Properties;
/*

 TOPIC_NAME=avro-simple-topic
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

public class SimpleAvro {

    @Test
    public void produce(){
        Common common = new Common();
        Properties props = common.createProcessorProducerProperties(null);
        // Override Value Serializer to be Avro
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // Override the default Key and Value name strategy
        props.put(AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);

        KafkaProducer<String, ZombieWeapon> producer = new KafkaProducer<>(props);
        for (int cnt=0; cnt < 10; cnt++) {
            Axe weapon = new Axe(8 + cnt);
            ZombieWeapon zombieWeapon = new ZombieWeapon("Axe", weapon);
            ProducerRecord<String, ZombieWeapon> record = new ProducerRecord<>("avro-simple-topic", "fight! Round " + cnt, zombieWeapon);
            producer.send(record);

        }
        System.out.println ("Sent messages.");
        producer.flush();
        producer.close();
    }

    @Test
    public void consume() {
        Common common = new Common();
        Properties props = common.createStreamsClientConfiguration("simple-avro-v1.0.0", "avroClient");

        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ZombieWeapon> zombieWeaponKStream = builder.stream("avro-simple-topic");

        zombieWeaponKStream.foreach((key, zombieWeapon) -> System.out.println("'key: " + key + "', weapon: " + zombieWeapon));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        for(;;) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {}
        }
    }
}
