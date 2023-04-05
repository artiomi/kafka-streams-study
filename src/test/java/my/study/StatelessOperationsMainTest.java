package my.study;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatelessOperationsMainTest {

  TopologyTestDriver testDriver;
  TestOutputTopic<String, String> outputTopic;

  @BeforeEach
  void beforeEach() {
    initTestDriver();
    initOutputTopic();
  }

  @Test
  void processSuccessfully() {
    publish("one", "first");
    KeyValue<String, String> keyValue = outputTopic.readKeyValue();
    assertEquals("one", keyValue.key);
    assertEquals("FIRST", keyValue.value);
  }

  @Test
  void filterShortValues() {
    publish("two", "two");
    publish("three", "third");
    List<KeyValue<String, String>> keyValues = outputTopic.readKeyValuesToList();
    assertEquals(1, keyValues.size());
    assertEquals("three", keyValues.get(0).key);
    assertEquals("THIRD", keyValues.get(0).value);
  }

  @Test
  void testPunctuation() {
    testDriver.advanceWallClockTime(Duration.ofMillis(20L));
    publish("four", "four");
    TestRecord<String, String> record = outputTopic.readRecord();
    System.out.println(record);
    assertEquals("four", record.getKey());
    assertEquals("FOUR", record.getValue());
  }

  private void publish(String key, String value) {
    TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(
        "input-topic",
        new StringSerializer(),
        new StringSerializer());
    inputTopic.pipeInput(key, value);
  }

  private void initOutputTopic() {
    outputTopic = testDriver.createOutputTopic(
        "output-topic",
        new StringDeserializer(),
        new StringDeserializer());
  }

  private void initTestDriver() {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> stream = builder
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("aggStore"),
            Serdes.String(),
            Serdes.Long()).withLoggingDisabled())
        .stream("input-topic");
    stream.filter((key, value) -> value.length() > 3, Named.as("filter_value_gt_3"))
        .mapValues(value -> value.toUpperCase(), Named.as("map_value_uppercase"))
        .to("output-topic");
    Topology topology = builder.build();

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    testDriver = new TopologyTestDriver(topology, config);
    System.out.println("topology:\n" + topology.describe());
    System.out.println("store:\n"+testDriver.getAllStateStores());

  }


  @AfterEach
  void afterEach() {
    testDriver.close();
  }

}
