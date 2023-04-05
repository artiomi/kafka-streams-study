package my.study.common;


import java.time.Instant;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingProcessorSupplier<KIn, VIn extends VOut, VOut> implements
    FixedKeyProcessorSupplier<KIn, VIn, VOut> {

  private static final Logger logger = LoggerFactory.getLogger(LoggingProcessorSupplier.class);

  @Override
  public FixedKeyProcessor<KIn, VIn, VOut> get() {
    return new LoggingProcessor<>();
  }

  private static final class LoggingProcessor<KIn, VIn extends VOut, VOut> extends
      ContextualFixedKeyProcessor<KIn, VIn, VOut> {

    @Override
    public void process(FixedKeyRecord<KIn, VIn> record) {
      logger.info("Record. Key:[{}], timestamp:[{}], value:[{}]", record.key(),
          Instant.ofEpochMilli(record.timestamp()), record.value());
      context().forward(record);
    }
  }
}
