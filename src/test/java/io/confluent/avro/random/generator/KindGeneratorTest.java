package io.confluent.avro.random.generator;

import com.telefonica.baikal.utils.Validations;
import io.confluent.avro.random.generator.util.ResourceUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.validator.routines.EmailValidator;
import org.junit.Test;

import java.util.UUID;

import static io.confluent.avro.random.generator.util.ResourceUtil.generateRecordWithSchema;
import static org.junit.Assert.*;


public class KindGeneratorTest {

  private static final String UUID_SCHEMA =
      ResourceUtil.loadContent("test-schemas/kinds/uuid.json");

  @Test
  public void shouldCreateValidUUIDForStrings() {
    GenericRecord record = generateRecordWithSchema("test-schemas/kinds/uuid.json");

    String field = "user_id";
    assertNotNull(record.get(field));
    String value = record.get(field).toString();
    System.out.println("Generated value is: " + value);

    try {
      UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      fail("user_id is not a valid UUID: " + value);
    }
  }

  @Test
  public void shouldCreateValidNameForStrings() {
    GenericRecord record = generateRecordWithSchema("test-schemas/kinds/name.json");

    String field = "name";
    assertNotNull(record.get(field));
    String value = record.get(field).toString();
    System.out.println("Generated value is: " + value);
  }

  @Test
  public void shouldCreateValidEmailForStrings() {
    GenericRecord record = generateRecordWithSchema("test-schemas/kinds/email.json");

    String field = "email";
    assertNotNull(record.get(field));
    String value = record.get(field).toString();
    System.out.println("Generated value is: " + value);
    assertTrue("Invalid iso-date: " + value, EmailValidator.getInstance().isValid(value));
  }

}
