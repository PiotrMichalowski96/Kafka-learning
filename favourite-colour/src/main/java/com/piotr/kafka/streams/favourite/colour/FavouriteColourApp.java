package com.piotr.kafka.streams.favourite.colour;

import com.piotr.kafka.streams.favourite.colour.util.Colour;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class FavouriteColourApp {

  private static final String COLOUR_TOPIC = "input-colour-topic";
  private static final String INTERMEDIARY_TOPIC = "user-keys-and-colours";
  private static final String OUTPUT_TOPIC = "output-colour-topic";

  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-favourite-colour-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // we disable the cache to demonstrate all the steps involved in the transformation - not recommended in prod
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> colourStream = builder.stream(COLOUR_TOPIC);

    colourStream
        .filter((key, value) -> value.contains(","))
        .selectKey((ignoredKey, userAndColour) -> getUser(userAndColour))
        .mapValues(FavouriteColourApp::getColour)
        .filter((user, colour) -> Colour.getAnyCorrectColourPredicate().test(colour))
        .to(INTERMEDIARY_TOPIC);

    KTable<String, String> colourTable = builder.table(INTERMEDIARY_TOPIC);

    colourTable
        .groupBy((user, colour) -> KeyValue.pair(colour, colour))
        .count(Materialized.as("Count-colours"))
        .toStream()
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.start();

    System.out.println(streams.toString());

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static String getUser(String userAndColour) {
    return userAndColour.trim().split("\\s*,\\s*")[0];
  }

  private static String getColour(String userAndColour) {
    return userAndColour.trim().split("\\s*,\\s*")[1];
  }
}
