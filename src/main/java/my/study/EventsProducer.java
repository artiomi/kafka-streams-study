package my.study;

import static my.study.common.PropertiesProvider.getProducerProperties;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class EventsProducer {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Properties properties = getProducerProperties();

    CompletableFuture<Void> future1 = CompletableFuture.runAsync(
        () -> publishToInputTopic((Properties) properties.clone()));
    CompletableFuture<Void> future2 = CompletableFuture.runAsync(
        () -> publishToInputTopicDetails((Properties) properties.clone()));
    future1.get();
    future2.get();
    System.out.println("event production completed");
  }


  private static void publishToInputTopicDetails(Properties properties) {
    System.out.println("Start producing events to input-topic-details");
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
      TimeUnit.MILLISECONDS.sleep(0);

//      producer.send(newStringEvent("john"));
//      TimeUnit.MILLISECONDS.sleep(1000);
      producer.send(newStringEvent("1"));
      producer.send(newStringEvent("2"));
      producer.send(newStringEvent("3"));

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Complete producing events to input-topic-details");
  }

  private static void publishToInputTopic(Properties properties) {
    System.out.println("Start producing events to input-topic");

    try (Producer<String, Long> producer = new KafkaProducer<>(properties)) {

      TimeUnit.MILLISECONDS.sleep(0);
      producer.send(newRandomEvent("john"));
      producer.send(newRandomEvent("alex"));

//      producer.send(newRandomEvent("alice"));
      producer.send(newRandomEvent("mike"));
//      producer.send(newRandomEvent("alice"));
//      producer.send(newRandomEvent("john"));
//      producer.send(newRandomEvent("mike"));
//      producer.send(newRandomEvent("john"));
//      producer.send(newRandomEvent("john"));
//      producer.send(newRandomEvent("mike"));

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Complete producing events to input-topic");

  }

  public static ProducerRecord<String, Long> newRandomEvent(String key) {

    Long value = ThreadLocalRandom.current().nextLong(1, 4);

    return new ProducerRecord<>("input-topic", key, value);
  }

  public static ProducerRecord<String, Long> tombstone(String key) {

    return new ProducerRecord<>("input-topic", key, null);
  }

  public static ProducerRecord<String, String> newStringEvent(String key) {
    String value = RandomStringUtils.randomAlphabetic(5);

    return new ProducerRecord<>("input-topic-details", key, value);
  }
}
