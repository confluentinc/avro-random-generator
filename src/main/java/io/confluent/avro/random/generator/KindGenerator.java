package io.confluent.avro.random.generator;

import com.github.javafaker.Faker;

import java.util.UUID;

public class KindGenerator {

  /**
   * The encoding of the type. Must be given as a string.
   */
  public static final String KIND_PROP = "kind";


  private static final Faker faker = new Faker();

  public static String random(Object kindProp) {

    if (!(kindProp instanceof String)) {
      throw new RuntimeException(String.format("%s property must be a string", KIND_PROP));
    }

    switch ((String)kindProp) {
      case "uuid": return faker.internet().uuid();
      case "name": return faker.name().name();
      case "email": return faker.internet().safeEmailAddress();
      default:
        throw new RuntimeException(String.format("Unknown %s property: %s", KIND_PROP, kindProp));
    }
  }
}
