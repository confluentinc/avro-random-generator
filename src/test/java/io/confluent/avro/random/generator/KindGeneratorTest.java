package io.confluent.avro.random.generator;

import io.confluent.avro.random.generator.util.ResourceUtil;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;


public class KindGeneratorTest {

  private static final String UUID_SCHEMA =
      ResourceUtil.loadContent("test-schemas/kinds/uuid.json");

  @Test
  public void shouldCreateUUIDForStrings() {
    Generator generator = new Generator.Builder().schemaString(UUID_SCHEMA).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("user_id"));
    try {
      UUID.fromString(generated.get("user_id").toString());
    } catch (IllegalArgumentException e) {
      fail("user_id is not a valid UUID: " + generated.get("user_id").toString());
    }
  }

}
