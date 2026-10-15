/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.avro.random.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class MaxLengthTest {

  private static String bytesSchema(String length) {
    return "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"f\",\"type\":"
        + "{\"type\":\"bytes\",\"arg.properties\":{\"length\":" + length + "}}}]}";
  }

  private static Generator generator(String length, Integer maxLength) {
    Generator.Builder builder = new Generator.Builder().schemaString(bytesSchema(length));
    if (maxLength != null) {
      builder.maxLength(maxLength);
    }
    return builder.build();
  }

  @Test(expected = RuntimeException.class)
  public void shouldRejectLengthAboveDefaultMax() {
    generator("{\"min\":2000000000,\"max\":2000000001}", null).generate();
  }

  @Test
  public void shouldHonorConfiguredMaxLength() {
    GenericRecord record = (GenericRecord) generator("100", 100).generate();
    assertEquals(100, ((ByteBuffer) record.get("f")).remaining());
  }

  @Test(expected = RuntimeException.class)
  public void shouldRejectLengthAboveConfiguredMax() {
    generator("101", 100).generate();
  }

  @Test
  public void shouldCapOpenEndedRange() {
    GenericRecord record = (GenericRecord) generator("{\"min\":10}", 100).generate();
    int size = ((ByteBuffer) record.get("f")).remaining();
    assertTrue(size >= 10 && size <= 100);
  }
}
