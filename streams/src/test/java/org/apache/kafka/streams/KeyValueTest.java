/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(600)
public class KeyValueTest {
    @Test
    public void shouldHaveSameEqualsAndHashCode() {
        final KeyValue<String, Long> kv = KeyValue.pair("key1", 1L);
        final KeyValue<String, Long> copyOfKV = KeyValue.pair(kv.key, kv.value);

        // Reflexive
        Assertions.assertTrue(kv.equals(kv));
        Assertions.assertTrue(kv.hashCode() == kv.hashCode());

        // Symmetric
        Assertions.assertTrue(kv.equals(copyOfKV));
        Assertions.assertTrue(kv.hashCode() == copyOfKV.hashCode());
        Assertions.assertTrue(copyOfKV.hashCode() == kv.hashCode());

        // Transitive
        final KeyValue<String, Long> copyOfCopyOfKV = KeyValue.pair(copyOfKV.key, copyOfKV.value);
        Assertions.assertEquals(copyOfKV, copyOfCopyOfKV);
        Assertions.assertEquals(copyOfKV.hashCode(), copyOfCopyOfKV.hashCode());
        Assertions.assertEquals(kv, copyOfCopyOfKV);
        Assertions.assertEquals(kv.hashCode(), copyOfCopyOfKV.hashCode());

        // Inequality scenarios
        Assertions.assertNotEquals(null, kv, "must be false for null");
        Assertions.assertNotEquals(kv, KeyValue.pair(null, kv.value), "must be false if key is non-null and other key is null");
        Assertions.assertNotEquals(kv, KeyValue.pair(kv.key, null), "must be false if value is non-null and other value is null");
        final KeyValue<Long, Long> differentKeyType = KeyValue.pair(1L, kv.value);
        Assertions.assertNotEquals(kv, differentKeyType, "must be false for different key types");
        final KeyValue<String, String> differentValueType = KeyValue.pair(kv.key, "anyString");
        Assertions.assertNotEquals(kv, differentValueType, "must be false for different value types");
        final KeyValue<Long, String> differentKeyValueTypes = KeyValue.pair(1L, "anyString");
        Assertions.assertNotEquals(kv, differentKeyValueTypes, "must be false for different key and value types");
        Assertions.assertNotEquals(kv, new Object(), "must be false for different types of objects");

        final KeyValue<String, Long> differentKey = KeyValue.pair(kv.key + "suffix", kv.value);
        Assertions.assertNotEquals(kv, differentKey, "must be false if key is different");
        Assertions.assertNotEquals(differentKey, kv, "must be false if key is different");

        final KeyValue<String, Long> differentValue = KeyValue.pair(kv.key, kv.value + 1L);
        Assertions.assertNotEquals(kv, differentValue, "must be false if value is different");
        Assertions.assertNotEquals(differentValue, kv, "must be false if value is different");

        final KeyValue<String, Long> differentKeyAndValue = KeyValue.pair(kv.key + "suffix", kv.value + 1L);
        Assertions.assertNotEquals(kv, differentKeyAndValue, "must be false if key and value are different");
        Assertions.assertNotEquals(differentKeyAndValue, kv, "must be false if key and value are different");
    }

}