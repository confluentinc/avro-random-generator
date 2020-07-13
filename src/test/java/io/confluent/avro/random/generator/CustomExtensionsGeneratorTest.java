package io.confluent.avro.random.generator;

import org.apache.avro.generic.GenericRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static io.confluent.avro.random.generator.util.ResourceUtil.builderWithSchema;
import static io.confluent.avro.random.generator.util.ResourceUtil.generateRecordWithSchema;
import static org.junit.Assert.*;


public class CustomExtensionsGeneratorTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void shouldCreateUnionWithPosition0() {
    GenericRecord record = generateRecordWithSchema("test-schemas/extensions/unions-position-0.json");

    String field = "nullable_col";
    assertNull(record.get(field));
  }

  @Test
  public void shouldCreateUnionWithPosition1() {
    GenericRecord record = generateRecordWithSchema("test-schemas/extensions/unions-position-1.json");

    String field = "nullable_col";
    assertNotNull(record.get(field));
    String value = record.get(field).toString();
    System.out.println("Generated value is: " + value);
  }

  @Test
  public void shouldCreateUnionWithDistributedProv() {
    Generator generator = builderWithSchema("test-schemas/extensions/unions-distribution.json");

    GenericRecord record;
    List<Object> results = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      record = (GenericRecord) generator.generate();
      Object col = record.get("nullable_col");
      if (col != null) {
        results.add(col);
      }
    }

    assertEquals("", 0.7, ((double) results.size()) / 100, 0.1);
  }

  @Test
  public void shouldFailIfTheDistributionIfMalformed() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("all probabilities of distribution property must sum 1");
    generateRecordWithSchema("test-schemas/extensions/unions-distribution-error.json");
  }

}
