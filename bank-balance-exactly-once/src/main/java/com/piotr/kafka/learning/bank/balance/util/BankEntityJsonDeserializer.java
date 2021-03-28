package com.piotr.kafka.learning.bank.balance.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
@RequiredArgsConstructor
public class BankEntityJsonDeserializer<T> implements Deserializer<T> {

  private final Class<T> clazz;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    try {
      return mapper.readValue(bytes, clazz);
    } catch (IOException e) {
      logger.error("Couldn't deserialize json of bank entity object: {}", clazz.getSimpleName());
      return null;
    }
  }

  @Override
  public void close() {
  }
}
