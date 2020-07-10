package io.confluent.avro.random.generator;

import io.confluent.avro.random.generator.util.ResourceUtil;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;

import static org.junit.Assert.*;


public class LogicalTypeGeneratorTest {

  private static final String ISO_DATE_SCHEMA =
      ResourceUtil.loadContent("test-schemas/logical-types/iso-date.json");

  private static final String ISO_DATE_SCHEMA_WITH_RANGE =
          ResourceUtil.loadContent("test-schemas/logical-types/iso-date-range.json");

  @Test
  public void shouldCreateValidISODate() {
    Generator generator = new Generator.Builder().schemaString(ISO_DATE_SCHEMA).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("day_dt"));
    try {
      LogicalTypeGenerator.ISO_DATE_FORMAT.parse((String) generated.get("day_dt"));
    } catch (ParseException e) {
      fail("day_dt is not in valid iso-date format (yyyy-MM-dd): " + generated.get("day_dt").toString());
    }
  }

  @Test
  public void shouldCreateValidISODateInRange() {
    Generator generator = new Generator.Builder().schemaString(ISO_DATE_SCHEMA_WITH_RANGE).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("day_dt"));
    try {
      Date start = LogicalTypeGenerator.ISO_DATE_FORMAT.parse("2020-01-01");
      Date end = LogicalTypeGenerator.ISO_DATE_FORMAT.parse("2020-02-01");
      Date date = LogicalTypeGenerator.ISO_DATE_FORMAT.parse((String) generated.get("day_dt"));
      assertTrue(date.after(start) && date.before(end));
    } catch (ParseException e) {
      fail("day_dt is not in valid iso-date format (yyyy-MM-dd): " + generated.get("day_dt").toString());
    }
  }

}
