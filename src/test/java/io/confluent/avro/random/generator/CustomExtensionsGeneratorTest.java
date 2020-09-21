package io.confluent.avro.random.generator;

import com.telefonica.baikal.utils.Validations;
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

    assertEquals("Wrong union distribution", 0.7, ((double) results.size()) / 100, 0.1);
  }

  @Test
  public void shouldFailIfTheDistributionIfMalformed() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("all probabilities of distribution property must sum 1");
    generateRecordWithSchema("test-schemas/extensions/unions-distribution-error.json");
  }

  @Test
  public void shouldCreateUniqueFieldValues() {
    Generator generator = builderWithSchema("test-schemas/extensions/unique-options.json");

    GenericRecord record;
    List<String> results = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      record = (GenericRecord) generator.generate();
      Object col = record.get("letter");
      if (col != null) {
        results.add((String) col);
      }
    }

    assertEquals("The number of generated records must be 3", 3, results.size(), 0);
    assertTrue("A result does not exists", results.contains("A"));
    assertTrue("B result does not exists", results.contains("B"));
    assertTrue("C result does not exists", results.contains("C"));
  }

  @Test
  public void shouldFailIfIsImpossibleGenerateUniqueValues() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("letter field options out of stock, it could be due to the generation of more records than possible unique values");

    Generator generator = builderWithSchema("test-schemas/extensions/unique-options.json");

    GenericRecord record;
    List<String> results = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      record = (GenericRecord) generator.generate();
      Object col = record.get("letter");
      if (col != null) {
        results.add((String) col);
      }
    }
  }

  @Test
  public void shouldCreateFieldWithMalformedRate() {
    Generator generator = builderWithSchema("test-schemas/extensions/malformed-distribution.json");

    GenericRecord record;
    List<Object> results = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      record = (GenericRecord) generator.generate();
      String col = (String) record.get("day_dt");
      if (Validations.isValidDate(col)) {
        results.add(col);
      }
    }

    assertEquals("Wrong malformed distribution", 0.7, ((double) results.size()) / 100, 0.1);
  }
}
