package se.ryz.kafka.demo;

/*
    This lab will not require any Java Code, only some work with the Containers runnkng Kafka.
    We will create a new Topic, write some data to it and have a look in the Kafka Log files.

    We will then remove one brokers log files, kill and restart it and see that the files have
    been restored again.


 Create a Topic with two partitions

    TOPIC_NAME=kafka-log

    # Delete Topic if it exists
    kafka-topics --delete \
    --if-exists \
    --topic $TOPIC_NAME \
    --zookeeper localhost:2181,localhost:2182,localhost:2183


    # Create Topic

    kafka-topics --create \
    --topic $TOPIC_NAME \
    --partitions 1 \
    --replication-factor 3 \
    --if-not-exists \
    --config min.insync.replicas=2 \
    --zookeeper localhost:2181,localhost:2182,localhost:2183

    # Kafkas log files are stored in /var/lib/kafka/data. Connect to the container for 'kafka-1' and
    # List files in that directory and see that the log file ending with '.log' is empty.
    docker exec kafka-1 bash -c "ls -al /var/lib/kafka/data/kafka-log-0"

    # Produce messages to Topic with console producer. Run the command below, type a few one-liners and exit with CTRL+C

    kafka-console-producer \
    --broker-list localhost:9092,localhost:9093,localhost:9094 \
    --topic $TOPIC_NAME

    # Now run interactive shell and see that we have data in the file ending with '.log'
    docker exec kafka-1 bash -c "ls -al /var/lib/kafka/data/kafka-log-0"

    # Now we can dump the content of the Kafka log file. You should see the data you produced earlier in the payload
    docker exec kafka-1 bash -c "kafka-run-class kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files /var/lib/kafka/data/kafka-log-0/00000000000000000000.log"

    # Remove all data files for the topic simulating disk error or similar
    docker exec kafka-1 bash -c "rm -rf /var/lib/kafka/data/kafka-log-0"

    # Check that files has been removed by trying to list directory again
    docker exec kafka-1 bash -c "ls -al /var/lib/kafka/data/kafka-log-0"


    # Kill Kafka container
    docker kill kafka-1

    # Start Kafka container again
    docker start kafka-1

    # List directory and see that the data is there
    docker exec kafka-1 bash -c "ls -al /var/lib/kafka/data/kafka-log-0"

    # And dump the log again
    docker exec kafka-1 bash -c "kafka-run-class kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files /var/lib/kafka/data/kafka-log-0/00000000000000000000.log"


 */
public class KafkaLog {
}
