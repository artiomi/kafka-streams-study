package my.study.stream.processor;

import static my.study.common.PropertiesProvider.getStreamProperties;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.ForeachProcessor;
import org.apache.kafka.streams.state.Stores;

@Slf4j
public class SampleProcessorMain {

  public static final String RESULT_LOG_MSG = "Result. Key:[{}], value:[{}]";
  public static final String INPUT_LOG_MSG = "Input. Key:[{}], value:[{}]";


  public static void main(String[] args) {
    Properties config = getStreamProperties("sample-processor-app", "sample-processor-config");
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
    Topology topology = new Topology();
    topology.addSource("topology-source", new StringDeserializer(), new StringDeserializer(), "click-topic")
        .addProcessor("input-logging", () -> new ForeachProcessor<>(logAction(INPUT_LOG_MSG)), "topology-source")
        .addProcessor("topology-processor", new WordCountProcessorSupplier(), "topology-source")
        .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("word-count-store"), Serdes.String(),
            Serdes.Integer()), "topology-processor")
        .addProcessor("foreach-out", () -> new ForeachProcessor<>(logAction(RESULT_LOG_MSG)), "topology-processor");

    return topology;
  }

  private static ForeachAction<?, ?> logAction(String logTemplate) {
    return (k, v) -> log.info(logTemplate, k, v);
  }
}
