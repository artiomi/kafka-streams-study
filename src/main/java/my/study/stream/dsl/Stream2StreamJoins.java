package my.study.stream.dsl;

import static my.study.common.PropertiesProvider.getStreamProperties;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stream2StreamJoins {

  private static final Logger logger = LoggerFactory.getLogger(Stream2StreamJoins.class);

  public static void main(String[] args) {
    Properties config = getStreamProperties("wordcount-application", "count-client");

    Topology topology = innerStream2StreamOuterJoin();
    KafkaStreams streams = new KafkaStreams(topology, config);
    logger.info("Topology:{}", topology.describe());

    streams.setUncaughtExceptionHandler(exception -> {
      logger.error("Try to handle exception.", exception);
      return StreamThreadExceptionResponse.REPLACE_THREAD;
    });
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static Topology innerStream2StreamOuterJoin() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> thisStream = builder.stream(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));

    KStream<String, String> otherStream = builder.stream(
        "input-topic-details",
        Consumed.with(
            Serdes.String(),
            Serdes.String()
        ));
    KStream<String, String> join = thisStream
        .peek((key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .outerJoin(otherStream
            , (value1, value2) -> String.format("user: %s details: %s", value1, value2)
            , JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(1500))
            , StreamJoined.<String, Long, String>with(
                    Stores.inMemoryWindowStore("in-memory-sore", Duration.ofSeconds(3), Duration.ofSeconds(3), true),
                    Stores.inMemoryWindowStore("in-memory-sore2", Duration.ofSeconds(3), Duration.ofSeconds(3), true))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
                .withOtherValueSerde(Serdes.String())
        );

    join.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

  private static Topology innerStream2StreamLeftJoin() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> thisStream = builder.stream(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));

    KStream<String, String> otherStream = builder.stream(
        "input-topic-details",
        Consumed.with(
            Serdes.String(),
            Serdes.String()
        ));
    KStream<String, String> join = thisStream
        .peek((key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .leftJoin(otherStream
            , (value1, value2) -> String.format("user: %s details: %s", value1, value2)
            , JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(1500))
            , StreamJoined.<String, Long, String>with(
                    Stores.inMemoryWindowStore("in-memory-sore", Duration.ofSeconds(3), Duration.ofSeconds(3), true),
                    Stores.inMemoryWindowStore("in-memory-sore2", Duration.ofSeconds(3), Duration.ofSeconds(3), true))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
                .withOtherValueSerde(Serdes.String())
        );

    join.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

  private static Topology innerStream2StreamJoin() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> thisStream = builder.stream(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));

    KStream<String, String> otherStream = builder.stream(
        "input-topic-details",
        Consumed.with(
            Serdes.String(),
            Serdes.String()
        ));
    KStream<String, String> join = thisStream
        .peek((key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .join(otherStream
            , (value1, value2) -> String.format("user: %s details: %s", value1, value2)
            , JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(1500))
            , StreamJoined.<String, Long, String>with(
                    Stores.inMemoryWindowStore("in-memory-sore", Duration.ofSeconds(3), Duration.ofSeconds(3), true),
                    Stores.inMemoryWindowStore("in-memory-sore2", Duration.ofSeconds(3), Duration.ofSeconds(3), true))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
                .withOtherValueSerde(Serdes.String())
        );

    join.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }


}
