package com.piotr.kafka.learning.bank.balance.util;

import com.piotr.kafka.learning.bank.balance.model.BankTransaction;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import lombok.experimental.UtilityClass;

@UtilityClass
public class BankTransactionDataGenerator {

  private static final double MIN_AMOUNT = 1000.0;
  private static final double MAX_AMOUNT = 10000.0;

  private static final Random random = new Random();

  private static final List<String> names = Arrays.asList("Peter", "John", "Emma", "Rachel", "Luis", "Marie");

  public static BankTransaction generateBankTransaction() {
    String name = randomName();
    BigDecimal amount = randomAmount();
    LocalDateTime time = LocalDateTime.now();
    return new BankTransaction(name, amount, time);
  }

  private static BigDecimal randomAmount() {
    double randomValue = MIN_AMOUNT + (MAX_AMOUNT - MIN_AMOUNT) * random.nextDouble();
    return BigDecimal.valueOf(randomValue);
  }

  private static String randomName() {
    int randomIndex = random.nextInt(names.size());
    return names.get(randomIndex);
  }
}
