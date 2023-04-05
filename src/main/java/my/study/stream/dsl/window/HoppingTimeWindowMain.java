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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
public class HoppingTimeWindowMain {

  public static void main(String[] args) {
    Properties config = getStreamProperties("hopping-window-app", "hopping-window-config");
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

    var clickTopic = builder.stream("click-topic",
        Consumed.with(Serdes.String(), Serdes.String()).withName("clicks-consumer"));

    KTable<Windowed<String>, Long> output = clickTopic
        .processValues(new LoggingProcessorSupplier<>(), Named.as("logging-processor"))
        .groupByKey(Grouped.as("click-group"))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(300), Duration.ofMillis(1000))
            .advanceBy(Duration.ofMillis(200)))
        .count(Named.as("clicks-count"),
// RocksDb attempt to use
//            Materialized.as(
//                Stores.persistentWindowStore("clicks-count-store", Duration.ofMillis(7000), Duration.ofMillis(5000), false)
//            )
//InMemory store supplier
            getWindowStoreMaterialized()
//InMemory DB
//            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(
//                "clicks-count-store").withStoreType(StoreType.IN_MEMORY)
        );

    output.toStream(Named.as("grouped-clicks-out-stream"))
        .foreach((k, v) -> log.info("Key:[{}], window:[start-{}/end-{}], value:[{}]", k.key(), k.window().startTime(),
                k.window().endTime(), v),
            Named.as("grouped-clicks-out"));

    return builder.build();
  }

  private static Materialized<String, Long, WindowStore<Bytes, byte[]>> getWindowStoreMaterialized() {
    Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized = Materialized.as(
        Stores.inMemoryWindowStore("clicks-count-store", Duration.ofMillis(7000), Duration.ofMillis(5000),
            false)
    );
    materialized.withKeySerde(Serdes.String());
    return materialized;
  }
}
