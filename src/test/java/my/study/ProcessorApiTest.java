package my.study;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessorApiTest {

  static Logger logger = LoggerFactory.getLogger(ProcessorApiTest.class);

  private final Serde<String> stringSerde = new Serdes.StringSerde();
  private final Serde<Long> longSerde = new Serdes.LongSerde();
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Long> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;
  private KeyValueStore<String, Long> store;

  @BeforeEach
  public void setup() {
    final Topology topology = new Topology();
    topology.addSource("sourceProcessor", "input-topic");
    topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("aggStore"),
            Serdes.String(),
            Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
        "aggregator");
    topology.addSink("sinkProcessor", "result-topic", "aggregator");

    // setup test driver
    final Properties props = new Properties();
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    testDriver = new TopologyTestDriver(topology, props);

    // setup test topics
    inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
    outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

    // pre-populate store
    store = testDriver.getKeyValueStore("aggStore");
    store.put("a", 21L);
    logger.info("topology:{}", topology.describe());
  }

  @Test
  public void shouldFlushStoreForFirstInput() {
    logger.info("before shouldFlushStoreForFirstInput()");
    inputTopic.pipeInput("a", 1L);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.isEmpty()).isTrue();
    logger.info("after shouldFlushStoreForFirstInput()");
  }

  @Test
  public void shouldNotUpdateStoreForLargerValue() {
    inputTopic.pipeInput("a", 42L);
    assertThat(store.get("a")).isEqualTo(42L);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 42L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  public void shouldPunctuateIfEvenTimeAdvances() {
    logger.info("before all shouldPunctuateIfEvenTimeAdvances()");
    final Instant recordTime = Instant.now();
    logger.info("before assert #1 shouldPunctuateIfEvenTimeAdvances()");
    inputTopic.pipeInput("a", 1L, recordTime);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));

    logger.info("before assert #2 shouldPunctuateIfEvenTimeAdvances()");
    inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(15));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));

    logger.info("before assert #2.1 shouldPunctuateIfEvenTimeAdvances()");
    inputTopic.pipeInput("a", 1L, recordTime);
    assertThat(outputTopic.isEmpty()).isTrue();

    logger.info("before assert #3 shouldPunctuateIfEvenTimeAdvances()");
    inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(30L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.isEmpty()).isTrue();
    logger.info("DONE shouldPunctuateIfEvenTimeAdvances()");
  }

  @Test
  public void shouldPunctuateIfEvenTimeIsNull() {
    logger.info("before all shouldPunctuateIfEvenTimeIsNull()");
    logger.info("before assert #1 shouldPunctuateIfEvenTimeIsNull()");
    inputTopic.pipeInput("a", 1L);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));

    logger.info("before assert #2 shouldPunctuateIfEvenTimeIsNull()");
    inputTopic.pipeInput("a", 1L);
    assertThat(outputTopic.isEmpty()).isTrue();

    logger.info("before assert #2.1 shouldPunctuateIfEvenTimeIsNull()");
    inputTopic.pipeInput("a", 1L);
    assertThat(outputTopic.isEmpty()).isTrue();

    logger.info("before assert #3 shouldPunctuateIfEvenTimeIsNull()");
    inputTopic.pipeInput("a", 1L);
    assertThat(outputTopic.isEmpty()).isTrue();
    logger.info("DONE shouldPunctuateIfEvenTimeIsNull()");
  }

  @Test
  public void shouldPunctuateIfWallClockTimeAdvances() {
    store.put("b", 22L);
    logger.info("before all shouldPunctuateIfWallClockTimeAdvances()");
    testDriver.advanceWallClockTime(Duration.ofSeconds(60));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", 22L));
    assertThat(outputTopic.isEmpty()).isTrue();
    logger.info("DONE shouldPunctuateIfWallClockTimeAdvances()");
  }


  @AfterEach
  void afterEach() {
    testDriver.close();
  }


  static class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long, String, Long> {

    @Override
    public Processor<String, Long, String, Long> get() {
      return new CustomMaxAggregator();
    }
  }

  static class CustomMaxAggregator implements Processor<String, Long, String, Long> {

    ProcessorContext<String, Long> context;
    KeyValueStore<String, Long> store;


    @Override
    public void init(ProcessorContext<String, Long> context) {
      logger.info("Processor - init() Context:{}", context);
      this.context = context;
      context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStoreWallClock());
      context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStoreStreamTime());
      store = context.getStateStore("aggStore");
    }

    private void flushStoreWallClock() {
      logger.info("Processor - flushStoreWallClock()");
      flushStore();
    }

    private void flushStoreStreamTime() {
      logger.info("Processor - flushStoreStreamTime()");
      flushStore();
    }

    private void flushStore() {
      try (KeyValueIterator<String, Long> it = store.all()) {
        while (it.hasNext()) {
          final KeyValue<String, Long> next = it.next();
          context.forward(new Record<>(next.key, next.value, Instant.now().toEpochMilli()));
          logger.info("Processor - flush:{}", next);
        }
      }
    }

    @Override
    public void process(Record<String, Long> record) {
      logger.info("Processor - process record:{}", record);
      final String key = record.key();
      final Long value = record.value();
      final Long oldValue = store.get(key);
      if (oldValue == null || value > oldValue) {
        store.put(key, value);
      }
    }

    @Override
    public void close() {
      logger.info("Processor - close()");
    }
  }
}