package se.ryz.kafka.demo;

/*
In this example we will send a message with a KafkaAvroSerializer and receive the message
with a client that reads messages as a byte array. We will see how Kafka references the Schema ID in the Schema Registry.
We will then deserialize the message as an Avro record without help from Kafka Avro lib.

 */
public class ManualSerde {
}
