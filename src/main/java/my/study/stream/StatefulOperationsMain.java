package my.study.stream;

import static my.study.common.PropertiesProvider.getStreamProperties;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatefulOperationsMain {

  private static final Logger logger = LoggerFactory.getLogger(StatefulOperationsMain.class);

  public static void main(String[] args) {
    Properties config = getStreamProperties("wordcount-application", "count-client");

    Topology topology = aggregateStreamSessionWindow();
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


  private static Topology aggregateStreamSessionWindow() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> inputTable = builder.stream(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));
    KTable<Windowed<String>, Long> outputTable = inputTable
        .peek((key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .groupByKey(Grouped.with("group-by-key", Serdes.String(), Serdes.Long()))
        .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(3), Duration.ofMinutes(5)))
        .reduce(Long::sum
            , Named.as("windowed-sum")
            , Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("windowed-counts-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
        );
    outputTable.toStream(Named.as("key-converter"))
        .selectKey((key, value) -> key.key() + " - start:" + key.window().startTime() + " end:" + key.window().endTime()
                + " hash:" + key.window().hashCode(),
            Named.as("update-key-add-window"))
        .to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }


  private static Topology aggregateStreamSlidingWindow() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> inputTable = builder.stream(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));
    KTable<Windowed<String>, Long> outputTable = inputTable
        .peek((key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .groupByKey(Grouped.with("group-by-key", Serdes.String(), Serdes.Long()))
        .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofMinutes(5)))
        .reduce(Long::sum
            , Named.as("windowed-sum")
            , Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("windowed-counts-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
        );
    outputTable.toStream(Named.as("key-converter"))
        .selectKey((key, value) -> key.key() + " - start:" + key.window().startTime() + " end:" + key.window().endTime()
                + " hash:" + key.window().hashCode(),
            Named.as("update-key-add-window"))
        .to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }

  private static Topology aggregateStreamTimeWindow() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> inputTable = builder.stream(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));
    KTable<Windowed<String>, Long> outputTable = inputTable
        .peek((key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .groupByKey(Grouped.with("group-by-key", Serdes.String(), Serdes.Long()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
        .reduce(Long::sum
            , Named.as("windowed-sum")
            , Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("windowed-counts-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
        );
    outputTable.toStream(Named.as("key-converter"))
        .selectKey((key, value) -> key.key() + " : " + key.window().toString(), Named.as("update-key-add-window"))
        .to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }

  private static Topology aggregateTable() {
    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, Long> inputTable = builder.table(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));
    KTable<String, Long> outputTable = inputTable
        .groupBy((String key, Long value) -> KeyValue.pair(key, value),
            Grouped.with("group-by-key", Serdes.String(), Serdes.Long()))
        .aggregate(
            () -> 0L
            , (key, newValue, aggregate) -> {
              var result = aggregate + newValue;
              logger.info("Adder called key:{} ,value:{}, aggregate:{}, result:{}", key, newValue, aggregate, result);
              return result;
            }
            , (key, oldValue, aggregate) -> {
              var result = aggregate - oldValue;
              logger.info("Substractor called key:{} ,value:{}, aggregate:{}, result:{}", key, oldValue, aggregate,
                  result);
              return result;
            }
            , Named.as("input-words-count")
            , Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
        );
    outputTable.toStream()
        .to("output-topic");

    return builder.build();
  }
}
