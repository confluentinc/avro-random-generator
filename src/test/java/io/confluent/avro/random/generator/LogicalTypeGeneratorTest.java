package io.confluent.avro.random.generator;

import com.telefonica.baikal.utils.Validations;
import io.confluent.avro.random.generator.util.ResourceUtil;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;

import static org.junit.Assert.*;


public class LogicalTypeGeneratorTest {

  @Test
  public void shouldCreateValidISODate() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/iso-date.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
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
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/iso-date-range.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
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

  @Test
  public void shouldCreateValidPhoneNumber() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/phone-number.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("caller_phone_with_prefix_id"));
    assertTrue("Invalid phone number: " + generated.get("caller_phone_with_prefix_id"),
            Validations.isValidPhoneNumber(generated.get("caller_phone_with_prefix_id").toString())
    );
  }

  @Test
  public void shouldCreateValidPhoneNumberInSpecificRegion() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/phone-number-region.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("caller_phone_with_prefix_id"));
    assertTrue("Invalid phone number: " + generated.get("caller_phone_with_prefix_id"),
            Validations.isValidPhoneNumber(generated.get("caller_phone_with_prefix_id").toString())
    );
    assertTrue("Invalid ES phone number: " + generated.get("caller_phone_with_prefix_id"),
            generated.get("caller_phone_with_prefix_id").toString().startsWith("+34"));
  }

  @Test
  public void shouldCreateValidCountryCodeNumeric() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/country-code-numeric.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("country"));
    assertTrue("Invalid country code: " + generated.get("country"),
            Validations.isValidCountryCodeNumeric(generated.get("country").toString())
    );
  }

  @Test
  public void shouldCreateValidCountryCodeAlpha2() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/country-code-alpha2.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("country"));
    assertTrue("Invalid country code alpha-2: " + generated.get("country"),
            Validations.isValidCountryCode2(generated.get("country").toString())
    );
  }

  @Test
  public void shouldCreateValidCountryCodeAlpha3() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/country-code-alpha3.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("country"));
    assertTrue("Invalid country code alpha-3: " + generated.get("country"),
            Validations.isValidCountryCode3(generated.get("country").toString())
    );
  }

  @Test
  public void shouldCreateValidCurrencyCodeNumeric() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/currency-code-numeric.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("currency"));
    assertTrue("Invalid currency code: " + generated.get("currency"),
            Validations.isValidCurrencyNumeric(generated.get("currency").toString())
    );
  }

  @Test
  public void shouldCreateValidCurrencyCodeAlpha() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/currency-code-alpha.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("currency"));
    assertTrue("Invalid currency code: " + generated.get("currency"),
            Validations.isValidCurrency(generated.get("currency").toString())
    );
  }

  @Test
  public void shouldCreateValidIMEI() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/imei.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("imei"));
    assertTrue("Invalid imei: " + generated.get("imei"),
            Validations.isValidIMEINumber(generated.get("imei").toString())
    );
  }

  @Test
  public void shouldCreateValidIMSI() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/imsi.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("imsi"));
    assertTrue("Invalid imsi: " + generated.get("imsi"),
            Validations.isValidIMSINumber(generated.get("imsi").toString())
    );
  }

  @Test
  public void shouldCreateValidDuration() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/duration.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("call_duration"));
    assertTrue("Invalid call_duration: " + generated.get("call_duration"),
            Validations.isValidDuration(generated.get("call_duration").toString())
    );
  }

  @Test
  public void shouldCreateValidTime() {
    String schema = ResourceUtil.loadContent("test-schemas/logical-types/time.json");
    Generator generator = new Generator.Builder().schemaString(schema).build();
    final GenericRecord generated = (GenericRecord) generator.generate();

    assertNotNull(generated.get("time"));
    assertTrue("Invalid time: " + generated.get("time"),
            Validations.isValidTime(generated.get("time").toString())
    );
  }

}
