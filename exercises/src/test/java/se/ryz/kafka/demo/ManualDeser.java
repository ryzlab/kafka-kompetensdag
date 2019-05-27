package se.ryz.kafka.demo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
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
import se.ryz.kafka.demo.util.Common;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

/*
In this example we will send a message with a KafkaAvroSerializer and receive the message
with a client that reads messages as a byte array. We will see how Kafka references the Schema ID in the Schema Registry.
We will then deserialize the message as an Avro record without help from Kafka Avro lib.

We will use the topic schema-registry-avro so create it first

kafka-topics --create \
 --topic schema-registry-avro \
 --partitions 1 \
 --replication-factor 1 \
 --if-not-exists \
 --config min.insync.replicas=1 \
 --zookeeper localhost:2181,localhost:2182,localhost:2183


Then run the producer, it will prduce 10 messages, and after that run the consumer and see that we can deserialize the
message after skipping the 5 byte header.
 */
public class ManualDeser {

    /**
     * First run the producer. It uses {@link KafkaAvroSerializer} to serialize the message to Avro.
     * The serializer will prepend a magic byte consisting of a 0 followed by 4 bytes that references the schema ID
     * to every record produced. Note that the producer sees nothing of the 5 byte header, it is hidden from
     * the producer under the covers.
     */
    @Test
    public void schemaFromFileProducer() {

        Common common = new Common();
        Properties props = common.createProcessorProducerProperties(null);
        // Override Value Serializer to be Avro
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);

        // Send a single message as Avro using the schema in the Schema Registry
        KafkaProducer<String, ZombieWeapon> producer = new KafkaProducer<>(props);
        Axe weapon = new Axe(8);
        ZombieWeapon zombieWeapon = new ZombieWeapon("Axe", weapon);
        ProducerRecord<String, ZombieWeapon> record = new ProducerRecord<>("schema-registry-avro", "Using Axe", zombieWeapon);
        producer.send(record);
        System.out.println ("Sent messages.");
        producer.flush();
        producer.close();

    }

    public static String toPrintableCharacters(byte[] a) {
        if (a == null)
            return "null";
        int iMax = a.length - 1;
        if (iMax == -1)
            return "[]";

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            if (Character.isLetterOrDigit((char)a[i])) {
                b.append((char)a[i]);
            } else {
                b.append(a[i]);
            }
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    @Test
    public void schemaFromFileConsumer() throws IOException, InterruptedException {

        // Note that we cannot use the schema from the file 'kafka-hello-world.avsc' that was used to create the stub file
        // used in the producer. When stub files are generated all records are in-lined and the resulting Avro schema
        // differs from the schema in the file.
        // Instead we have to use the schema from the string ZombieWeapon.SCHEMA$. The content of
        // that string is the same as the schema registered in the Schema Registry.
        Schema schema = ZombieWeapon.SCHEMA$;
        Common common = new Common();
        Properties streamsConfiguration = common.createStreamsClientConfiguration(this.getClass().getName() + "-application" + UUID.randomUUID().toString(), this.getClass().getName() + "-client");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Deserialize into a byte buffer
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, byte[]> messageStream = builder.stream("schema-registry-avro");
        messageStream.foreach((key, avroRecord) -> {

            System.out.println("Received message with " + avroRecord.length + " bytes. Key: " + key + ", avro record content: " + toPrintableCharacters(avroRecord));
            try {
                // We want to skip header
                int headerSize = 5;
                // We create the decoder and de-serialize into a GenericRecord after skipping the header
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroRecord, headerSize, avroRecord.length - headerSize, null);
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
                // Read the record
                GenericRecord record = datumReader.read(null, decoder);
                System.out.println("Avro record as JSON: " + record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Create and start the stream
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        while (streams.state() == KafkaStreams.State.RUNNING) {

            Thread.sleep(1000);
        }
    }

}
