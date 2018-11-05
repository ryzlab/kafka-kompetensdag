package se.ryz.kafka;

import org.apache.commons.cli.*;

public class HelloWorldService {

    public HelloWorldService(String kafkaBrokers, String schemaRegistry) {

    }

    public static void main(String[] args) throws ParseException{
        Options options = new Options();
        options.addOption(OptionBuilder
                .withArgName("k")
                .withLongOpt("kafka-brokers")
                .hasArg(true)
                .withDescription("The Kafka brokers. The broker address consists of 'host[:port]. If the optional port is not given, the default port 9092 is used")
                .isRequired()
        .create());
        options.addOption(OptionBuilder
                .withArgName("s")
                .withLongOpt("schema-registry")
                .hasArg(true)
                .withDescription("The host and optional port and protocol of the schema registry, ex: '[http[s]://}host[:port]. If the optional port is not given, the default port 8081 is used. If the optional protocol is not given, 'http' is used.")
                .isRequired()
        .create());

        CommandLineParser parser = new BasicParser();
        CommandLine cmdLine = parser.parse(options, args);
        String kafkaBrokers = cmdLine.getOptionValue("k");
        String schemaRegistry = cmdLine.getOptionValue("s");
        HelpFormatter hf = new HelpFormatter();
        if (kafkaBrokers == null || schemaRegistry == null) {
            hf.printHelp(HelloWorldService.class.getSimpleName(), options);
            System.exit(1);
        }
        new HelloWorldService(kafkaBrokers, schemaRegistry);
    }
}
