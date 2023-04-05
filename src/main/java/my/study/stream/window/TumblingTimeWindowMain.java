package my.study.stream.window;

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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

@Slf4j
public class TumblingTimeWindowMain {

  public static void main(String[] args) {
    Properties config = getStreamProperties("tumbling-window-app", "tumbling-window-config");
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
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(600), Duration.ofMillis(350)))
        .count(Named.as("clicks-count")
            ,Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(
                "clicks-count-store").withStoreType(StoreType.IN_MEMORY)
        );

    output.toStream().foreach(
        (k, v) -> log.info("Key:[{}], window:[start-{}/end-{}], value:[{}], window-interval:[{}]", k.key(), k.window().startTime(),
            k.window().endTime(), v, k.window()));

    return builder.build();
  }
}
