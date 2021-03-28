package com.piotr.kafka.learning.bank.balance.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class BankEntityJsonSerializer<T> implements Serializer<T> {

  @Override
  public void configure(Map configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String s, T o) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    try {
      return mapper.writeValueAsBytes(o);
    } catch (JsonProcessingException e) {
      logger.error("Couldn't serialize json of bank entity: {}", o.toString());
      return null;
    }
  }

  @Override
  public void close() {
  }
}
