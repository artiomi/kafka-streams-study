package my.study.stream.dsl.window;

import static my.study.common.PropertiesProvider.getStreamProperties;

import java.time.Duration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import my.study.common.LoggingProcessorSupplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Materialized.StoreType;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

@Slf4j
public class SessionWindowMain {

  public static void main(String[] args) {
    Properties config = getStreamProperties("session-window-app", "session-window-config");
    Topology topology = initTopology();
    KafkaStreams streams = new KafkaStreams(topology, config);
    log.info("Topology:{}", topology.describe());

    streams.setUncaughtExceptionHandler(exception -> {
      log.error("Try to handle exception.", exception);
      return StreamThreadExceptionResponse.REPLACE_THREAD;
    });
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static Topology initTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    var clickTopic = builder.stream("click-topic", Consumed.with(Serdes.String(), Serdes.String()));

    KTable<Windowed<String>, Long> output = clickTopic
        .processValues(new LoggingProcessorSupplier<>())
        .groupByKey(Grouped.as("click-group"))
        .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMillis(300), Duration.ofMillis(1000)))
        .count(Named.as("clicks-count"),
            Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(
                "clicks-count-store").withStoreType(StoreType.IN_MEMORY).withKeySerde(Serdes.String())
        );
    output
        .toStream(Named.as("instant-results-stream"))
        .foreach(
            (k, v) -> log.info("Intermediate response. Key:[{}], window:[start-{}/end-{}], value:[{}]", k.key(),
                k.window().startTime(),
                k.window().endTime(), v)
            , Named.as("instant-results-publisher")
        );

    output
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()).withName("click-suppress"))
        .toStream(Named.as("final-results-stream"))
        .foreach(
            (k, v) -> log.info("Final response. Key:[{}], window:[start-{}/end-{}], value:[{}]", k.key(),
                k.window().startTime(),
                k.window().endTime(), v)
            , Named.as("final-results-publisher")
        );

    return builder.build();
  }
}
