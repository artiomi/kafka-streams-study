package my.study.stream.dsl;

import static my.study.common.PropertiesProvider.getStreamProperties;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import my.study.common.LoggingProcessorSupplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Materialized.StoreType;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@Slf4j
public class QueryLocalKeyValueStoreMain {

  public static final String CLICKS_COUNT_STORE = "ClicksCountStore";
  public static final String COUNTS_FILTER_STORE = "CountsFilter";

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Properties config = getStreamProperties("query-local-store-app", "query-local-store-config");
    Topology topology = initTopology();
    KafkaStreams streams = new KafkaStreams(topology, config);
    log.info("Topology:{}", topology.describe());

    streams.setUncaughtExceptionHandler(exception -> {
      log.error("Try to handle exception.", exception);
      return StreamThreadExceptionResponse.REPLACE_THREAD;
    });
    streams.setStateListener((newState, oldState) -> {
      if (newState.isRunningOrRebalancing()) {
        log.info("State listener called for state:{}", newState);
        listDataFromStore(streams, CLICKS_COUNT_STORE);
        listDataFromStore(streams, COUNTS_FILTER_STORE);
        listKeyMetadata(streams);
      }
    });

    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static void listDataFromStore(KafkaStreams streams, String storeName) {
    ReadOnlyKeyValueStore<String, Long> countStore = streams.store(
        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    CompletableFuture.runAsync(() -> iterateStore(storeName, countStore));
  }

  private static void listKeyMetadata(KafkaStreams streams) {
    while (true) {
      KeyQueryMetadata bookMetadata = streams.queryMetadataForKey(CLICKS_COUNT_STORE, "book",
          Serdes.String().serializer());
      log.info("Key metadata:{}", bookMetadata);
      try {
        TimeUnit.SECONDS.sleep(10);
      } catch (InterruptedException e) {
        log.error("Interrupted thread.", e);
        return;
      }
    }
  }

  private static Topology initTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    var clickTopic = builder.stream("click-topic",
        Consumed.with(Serdes.String(), Serdes.String()).withName("clicks-consumer"));

    KTable<String, Long> output = clickTopic
        .processValues(new LoggingProcessorSupplier<>(), Named.as("logging-processor"))
        .groupByKey(Grouped.as("click-group"))
        .count(Named.as("clicks-counter"),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(CLICKS_COUNT_STORE)
                .withStoreType(StoreType.IN_MEMORY)
                .withValueSerde(Serdes.Long())
        );

    output
        .filter((key, value) -> key.equals("book"), Named.as("counts-filter"),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(COUNTS_FILTER_STORE)
                .withValueSerde(Serdes.Long())
                .withStoreType(StoreType.IN_MEMORY))
        .toStream(Named.as("grouped-clicks-out-stream"))
        .foreach((k, v) -> log.info("stream output key:[{}], value:[{}]", k, v), Named.as("grouped-clicks-out"));

    return builder.build();
  }

  private static void iterateStore(String storeName, ReadOnlyKeyValueStore<String, Long> store) {
    log.info("Init connection to store:{}", storeName);
    while (true) {
      log.info("Fetching data from store:{}", storeName);
      try (KeyValueIterator<String, Long> iterator = store.all()) {
        while (iterator.hasNext()) {
          KeyValue<String, Long> entry = iterator.next();
          log.info("{} count for key:[{}] is:[{}]", storeName, entry.key, entry.value);
        }
      }
      try {
        TimeUnit.SECONDS.sleep(10);
      } catch (InterruptedException e) {
        log.error("Interrupted thread.", e);
        return;
      }
    }

  }
}
