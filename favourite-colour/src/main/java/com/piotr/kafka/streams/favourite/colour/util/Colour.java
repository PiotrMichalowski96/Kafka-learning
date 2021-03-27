package com.piotr.kafka.streams.favourite.colour.util;

import java.util.function.Predicate;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum Colour {

  GREEN(colour -> colour.equalsIgnoreCase("green")),
  RED(colour -> colour.equalsIgnoreCase("red")),
  BLUE(colour -> colour.equalsIgnoreCase("blue"));

  private final Predicate<String> colourPredicate;

  public static Predicate<String> getAnyCorrectColourPredicate() {
    return GREEN.getColourPredicate().or(RED.getColourPredicate()).or(BLUE.getColourPredicate());
  }
}
