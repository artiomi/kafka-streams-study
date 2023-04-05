package my.study;

import static my.study.common.PropertiesProvider.getProducerProperties;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class EventsProducerForWindows {

  static Instant NOW = Instant.now();

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Properties properties = getProducerProperties();

    CompletableFuture<Void> future1 = CompletableFuture.runAsync(
        () -> publishToClickTopic((Properties) properties.clone()));
    future1.get();
    System.out.println("event production completed");
  }


  private static void publishToClickTopic(Properties properties) {
    System.out.println("Start producing events to click-topic");
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    System.out.println("Now variable: " + NOW);
    try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

      TimeUnit.MILLISECONDS.sleep(0);
      producer.send(newStringEvent("book", NOW.toEpochMilli() + 5));
      producer.send(newStringEvent("movie", NOW.toEpochMilli() + 6));
      TimeUnit.MILLISECONDS.sleep(100);
      producer.send(newStringEvent("book", NOW.toEpochMilli() + 1));
      TimeUnit.MILLISECONDS.sleep(100);
      producer.send(newStringEvent("movie", NOW.toEpochMilli() + 3));
      TimeUnit.MILLISECONDS.sleep(200);
      producer.send(newStringEvent("movie", NOW.toEpochMilli() + 2));
      TimeUnit.MILLISECONDS.sleep(200);
      producer.send(newStringEvent("movie", NOW.toEpochMilli()));

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Complete producing events to input-topic");

  }

  public static ProducerRecord<String, String> newStringEvent(String key, Long timestamp) {
    String value = RandomStringUtils.randomAlphabetic(5);

    return new ProducerRecord<>("click-topic", null, timestamp, key, value);
  }
}
