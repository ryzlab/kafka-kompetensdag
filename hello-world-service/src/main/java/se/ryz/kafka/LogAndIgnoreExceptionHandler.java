package se.ryz.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.log4j.Logger;

import java.util.Map;

public class LogAndIgnoreExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger logger = Logger.getLogger(LogAndIgnoreExceptionHandler.class);

    public LogAndIgnoreExceptionHandler() {

    }


    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {
        logger.error("Exception caught during deserialization, taskId: " + processorContext.taskId() +
                ", Topic: " + consumerRecord.topic() +
                ", partition: " + Integer.valueOf(consumerRecord.partition()) +
                ", offset: " + Long.valueOf(consumerRecord.offset()), e);
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
