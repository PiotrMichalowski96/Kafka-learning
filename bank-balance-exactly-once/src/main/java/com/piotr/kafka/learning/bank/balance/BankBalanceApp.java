package com.piotr.kafka.learning.bank.balance;

import com.piotr.kafka.learning.bank.balance.model.BankBalance;
import com.piotr.kafka.learning.bank.balance.model.BankTransaction;
import com.piotr.kafka.learning.bank.balance.util.BankEntityJsonDeserializer;
import com.piotr.kafka.learning.bank.balance.util.BankEntityJsonSerializer;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class BankBalanceApp {

  private static final String TRANSACTION_TOPIC = "input-bank-transaction";
  private static final String OUTPUT_BANK_BALANCE_TOPIC = "output-bank-balance";

  public static void main(String[] args) {
    new BankBalanceApp().run();
  }

  public void run() {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    final Serializer<BankTransaction> bankTransactionSerializer = new BankEntityJsonSerializer<>();
    final Deserializer<BankTransaction> bankTransactionDeserializer = new BankEntityJsonDeserializer<>(BankTransaction.class);
    final Serde<BankTransaction> bankTransactionSerde = Serdes.serdeFrom(bankTransactionSerializer, bankTransactionDeserializer);

    final Serializer<BankBalance> bankBalanceSerializer = new BankEntityJsonSerializer<>();
    final Deserializer<BankBalance> bankBalanceDeserializer = new BankEntityJsonDeserializer<>(BankBalance.class);
    final Serde<BankBalance> bankBalanceSerde = Serdes.serdeFrom(bankBalanceSerializer, bankBalanceDeserializer);

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, BankTransaction> transactions = builder.stream(TRANSACTION_TOPIC, Consumed.with(Serdes.String(), bankTransactionSerde));

    KTable<String, BankBalance> bankBalance = transactions
        .groupByKey(Grouped.with(Serdes.String(), bankTransactionSerde))
        .aggregate(
            () -> new BankBalance(0, BigDecimal.ZERO, LocalDateTime.MIN),
            (key, newTransaction, oldBalance) -> updateBalance(oldBalance, newTransaction),
            Materialized.<String, BankBalance, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                .withKeySerde(Serdes.String())
                .withValueSerde(bankBalanceSerde)
        );

    bankBalance.toStream().to(OUTPUT_BANK_BALANCE_TOPIC, Produced.with(Serdes.String(), bankBalanceSerde));

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.start();

    streams.localThreadsMetadata().forEach(System.out::println);

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private BankBalance updateBalance(BankBalance oldBalance, BankTransaction newTransaction) {
    int count = oldBalance.getCount() + 1;
    BigDecimal balance = oldBalance.getBalance().add(newTransaction.getAmount());
    LocalDateTime time = newTransaction.getTime();
    return new BankBalance(count, balance, time);
  }
}
