package my.study.stream.dsl;


import static my.study.common.PropertiesProvider.getStreamProperties;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatelessOperationsMain {

  private static final Logger logger = LoggerFactory.getLogger(StatelessOperationsMain.class);

  public static void main(String[] args) {
    Properties config = getStreamProperties("wordcount-application", "count-client");

    Topology topology = initUpdateKeyStream();
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

  private static Topology initStream() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> wordCounts = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Long()));
    KTable<String, Long> kTable = wordCounts.peek(
            (key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
//        .groupByKey(Grouped.as("group-by-key"))
        .groupBy((key, value) -> key, Grouped.with("group-by-key", Serdes.String(), Serdes.Long()))
        .aggregate(() -> 0L, (key, value, aggregate) -> aggregate + value, Named.as("input-words-count"),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
    kTable.toStream().to("output-topic");

    return builder.build();
  }

  private static Topology initUpdateKeyStream() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> wordCounts = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Long()));
    KStream<Integer, Long> out = wordCounts.peek(
            (key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .selectKey((key, value) -> key.length());

    out.to("output-topic", Produced.with(Serdes.Integer(), Serdes.Long()));

    return builder.build();
  }

  private static Topology initStreamRepartition() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> wordCounts = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Long()));
    KTable<String, Long> kTable = wordCounts.peek(
            (key, value) -> logger.info("Received key:[{}], value:[{}]", key, value)).repartition(
            Repartitioned.with(Serdes.String(), Serdes.Long()).withName("custom-repart").withNumberOfPartitions(6))
        .groupBy((key, value) -> key, Grouped.with("group-by-key", Serdes.String(), Serdes.Long()))
        .aggregate(() -> 0L, (key, value, aggregate) -> aggregate + value, Named.as("input-words-count"),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
    kTable.toStream().to("output-topic");

    return builder.build();
  }

  private static Topology initCogroupStream() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> longStream = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Long()));

    KStream<String, String> stringStream = builder.stream("input-topic-names",
        Consumed.with(Serdes.String(), Serdes.String()));
    KGroupedStream<String, Long> longGroup = longStream.peek(
            (key, value) -> logger.info("Received key:[{}], value:[{}]", key, value))
        .groupBy((key, value) -> key, Grouped.with("group-by-key-long", Serdes.String(), Serdes.Long()));

    KGroupedStream<String, String> stringGroup = stringStream.peek(
            (key, value) -> logger.info("Received2 key:[{}], value:[{}]", key, value))
        .groupBy((key, value) -> key, Grouped.with("group-by-key-string", Serdes.String(), Serdes.String()));

    KTable<String, String> kTable = longGroup.cogroup(
            (String key, Long value, String aggregate) -> aggregate + " first " + value)
        .cogroup(stringGroup, (key, value, aggregate) -> aggregate + " second " + value)
        .aggregate(() -> " ", Named.as("input-words-count"),
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("word-counts-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));

    kTable.toStream().to("output-topic");

    return builder.build();
  }

  private static Topology initStreamFlatMapValues() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> inStream = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Long()));
    KTable<String, Long> outStream = inStream.peek(
            (key, value) -> logger.info("Received key:[{}], value:[{}]", key, value), Named.as("logging-peek"))
        .flatMapValues(value -> IntStream.range(1, 4).mapToDouble(i -> Math.pow(10, i) * value)
                .mapToObj(value1 -> Double.valueOf(value1).longValue()).collect(Collectors.toList()),
            Named.as("multiply-100")).groupByKey(Grouped.with("char-group-by-key", Serdes.String(), Serdes.Long()))
        .count(Named.as("count-splited"));

    outStream.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
    return builder.build();
  }

  private static Topology initStreamFlatMap() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> inStream = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Long()));
    KTable<String, Long> outStream = inStream.peek(
            (key, value) -> logger.info("Received key:[{}], value:[{}]", key, value), Named.as("logging-peek")).flatMap(
            (String key, Long value) -> IntStream.range(0, key.length())
                .mapToObj(i -> String.valueOf(key.charAt(i)).toUpperCase()).map(k -> KeyValue.pair(k.toUpperCase(), value))
                .collect(Collectors.toList()), Named.as("to-char-key"))
        .groupByKey(Grouped.with("group-by-key", Serdes.String(), Serdes.Long()))
//        .groupBy((key, value) -> value % 10, Grouped.with("group-by-value-module", Serdes.Long(), Serdes.Long()))
        .count(Named.as("count-splited"));

    outStream.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
    return builder.build();
  }

  private static Topology initBranchedStream() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Long> wordCounts = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Long()));
    Map<String, KStream<String, Long>> branchedStream = wordCounts.peek(
            (key, value) -> logger.info("Received key:[{}], value:[{}]", key, value)).split(Named.as("Branch-"))
        .branch((key, value) -> value < 25, Branched.as("lt-25"))
        .branch((key, value) -> value < 50, Branched.as("lt-50")).defaultBranch(Branched.as("unfiltered"));
    branchedStream.get("Branch-lt-25").to("output-topic");
    branchedStream.get("Branch-lt-50")
        .foreach((key, value) -> logger.info("Branch-lt-50. key:[{}], value:[{}]", key, value));
    branchedStream.get("Branch-unfiltered")
        .foreach((key, value) -> logger.info("Branch-unfiltered. key:[{}], value:[{}]", key, value));

    return builder.build();
  }

  private static void initGlobalKTable() {
    StreamsBuilder builder = new StreamsBuilder();

    GlobalKTable<String, Long> wordCounts = builder.globalTable("word-counts-input-topic",
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts-global-store" /* table/store name */)
            .withKeySerde(Serdes.String()) /* key serde */.withValueSerde(Serdes.Long()) /* value serde */);
  }
}