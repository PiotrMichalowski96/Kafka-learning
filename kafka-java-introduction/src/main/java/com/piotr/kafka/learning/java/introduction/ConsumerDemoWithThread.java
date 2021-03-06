package com.piotr.kafka.learning.java.introduction;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConsumerDemoWithThread {

  public static void main(String[] args) {
    new ConsumerDemoWithThread().run();
  }

  public void run() {
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-thread-app";
    String topic = "first_topic";

    CountDownLatch latch = new CountDownLatch(1);

    Runnable myConsumerRunnable = new ConsumerRunnable(
        latch,
        bootstrapServers,
        groupId,
        topic
    );

    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread( () -> {
      logger.info("Caught shutdown hook");
      ((ConsumerRunnable) myConsumerRunnable).shutDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      logger.info("Application has exited");
    }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application got interrupted", e);
    } finally {
      logger.info("Application is closing");
    }
  }

  public class ConsumerRunnable implements Runnable {

    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(CountDownLatch latch,
                          String bootstrapServers,
                          String groupId,
                          String topic) {
      this.latch = latch;

      // create consumer configs
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      // create consumer
      consumer = new KafkaConsumer<String, String>(properties);

      // subscribe consumer to out topic(s)
      consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
      // poll for new data
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

          for (ConsumerRecord<String, String> record : records) {
            logger.info("Key: {}, Value: {}", record.key(), record.value());
            logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
          }
        }
      } catch (WakeupException e) {
        logger.info("Received shutdown signal!");
      } finally {
        consumer.close();
        // tell our main code we're done with the consumer
        latch.countDown();
      }
    }

    public void shutDown() {
      // the wakeup() method is a special method to interrupt consumer.poll()
      // it will throw the exception WakeupException
      consumer.wakeup();
    }
  }
}
