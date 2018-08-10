package io.confluent.avro.random.generator;

import org.junit.Test;

import java.util.Random;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class AlwaysIncludeOptionalsTest {
  final Random RNG = new Random();
  final String SCHEMA = "[\"null\", \"string\"]";
  final int ITERATIONS = 100;

  @Test
  public void shouldAlwaysGenerateOptional() {
    final Generator generator = new Generator(SCHEMA, RNG, true);
    IntStream.range(0, ITERATIONS).forEach(i -> assertThat(generator.generate(), notNullValue()));
  }
}
