package my.study.stream;

import static my.study.common.PropertiesProvider.getStreamProperties;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Table2TableForeignKeyJoins {

  private static final Logger logger = LoggerFactory.getLogger(Table2TableForeignKeyJoins.class);

  public static void main(String[] args) {
    Properties config = getStreamProperties("foreign-key-joins-application", "count-client");

    Topology topology = foreignKeyTable2TableInnerJoin();
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

  private static Topology foreignKeyTable2TableInnerJoin() {
    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, Long> thisTable = builder.table(
        "input-topic",
        Consumed.with(
            Serdes.String(),
            Serdes.Long()
        ));

    KTable<String, String> otherTable = builder.table(
        "input-topic-details",
        Consumed.with(
            Serdes.String(),
            Serdes.String()
        ));
    KTable<String, String> join = thisTable
        .join(otherTable
            , aLong -> String.valueOf(aLong)
            , (value1, value2) -> String.format("user: %s details: %s", value1, value2)
            , TableJoined.as("foreign-key-table")
            , Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("foreign-key-store")
                .withKeySerde(Serdes.String())
                .withKeySerde(Serdes.String())
        );

    join.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }


}
