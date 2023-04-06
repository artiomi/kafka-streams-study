package my.study.stream.processor;

import java.time.Duration;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class WordCountProcessorSupplier implements ProcessorSupplier<String, String, String, String> {

  @Override
  public Processor<String, String, String, String> get() {
    return new WordCountProcessor();
  }

  private static class WordCountProcessor implements Processor<String, String, String, String> {

    private KeyValueStore<String, Integer> kvStore;

    @Override
    public void init(final ProcessorContext<String, String> context) {
      log.info("initialize context:[{}]", this.getClass().getSimpleName());
      kvStore = context.getStateStore("word-count-store");
      context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
        log.info("call schedule for context:[{}]", this.getClass().getSimpleName());
        try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
          while (iter.hasNext()) {
            final KeyValue<String, Integer> entry = iter.next();
            log.info("schedule:[{}] forward record:{}", this.getClass().getSimpleName(), entry);
            context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
          }
        }
      });
    }

    @Override
    public void process(Record<String, String> record) {
      log.info("[{}] process record:{}", this.getClass().getSimpleName(), record);

      final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\\W+");

      for (final String word : words) {
        final Integer oldValue = kvStore.get(word);

        if (oldValue == null) {
          kvStore.put(word, 1);
        } else {
          kvStore.put(word, oldValue + 1);
        }
      }
    }
  }
}