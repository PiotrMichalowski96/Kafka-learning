package com.piotr.kafka.learning.bank.balance;

import static com.piotr.kafka.learning.bank.balance.util.BankTransactionDataGenerator.generateBankTransaction;

import com.piotr.kafka.learning.bank.balance.model.BankTransaction;
import com.piotr.kafka.learning.bank.balance.util.BankEntityJsonSerializer;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class BankTransactionProducer {

  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
  private static final String TRANSACTION_TOPIC = "input-bank-transaction";

  public static void main(String[] args) {
    new BankTransactionProducer().run();
  }

  public void run() {

    KafkaProducer<String, BankTransaction> producer = createKafkaProducer();

    int sentBatch = 0;
    while (true) {
      logger.info("Producing batch: {}", sentBatch);
      try {
        createAndSendRecord(producer);
        Thread.sleep(100);
        createAndSendRecord(producer);
        Thread.sleep(100);
        createAndSendRecord(producer);
        Thread.sleep(100);
        sentBatch++;
      } catch (InterruptedException e) {
        break;
      }
    }

    producer.close();
  }

  private KafkaProducer<String, BankTransaction> createKafkaProducer() {

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    final Serializer<String> stringSerializer = new StringSerializer();
    final Serializer<BankTransaction> bankTransactionSerializer = new BankEntityJsonSerializer<>();

    return new KafkaProducer<>(properties, stringSerializer, bankTransactionSerializer);
  }

  private void createAndSendRecord(KafkaProducer<String, BankTransaction> producer) {
    BankTransaction bankTransaction = generateBankTransaction();
    String key = bankTransaction.getName();

    ProducerRecord<String, BankTransaction> record = new ProducerRecord<>(TRANSACTION_TOPIC, key, bankTransaction);

    producer.send(record, (recordMetadata, exception) -> {
      if (exception != null) {
        logger.error("Something bad happened", exception);
      }
    });
  }
}
