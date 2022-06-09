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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
public abstract class AbstractKeyValueStoreTest {

    protected abstract <K, V> KeyValueStore<K, V> createKeyValueStore(final StateStoreContext context);

    protected InternalMockProcessorContext context;
    protected KeyValueStore<Integer, String> store;
    protected KeyValueStoreTestDriver<Integer, String> driver;

    @BeforeEach
    public void before() {
        driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        context = (InternalMockProcessorContext) driver.context();
        context.setTime(10);
        store = createKeyValueStore(context);
    }

    @AfterEach
    public void after() {
        store.close();
        driver.clear();
    }

    private static Map<Integer, String> getContents(final KeyValueIterator<Integer, String> iter) {
        final HashMap<Integer, String> result = new HashMap<>();
        while (iter.hasNext()) {
            final KeyValue<Integer, String> entry = iter.next();
            result.put(entry.key, entry.value);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotIncludeDeletedFromRangeResult() {
        store.close();

        final Serializer<String> serializer = new StringSerializer() {
            private int numCalls = 0;

            @Override
            public byte[] serialize(final String topic, final String data) {
                if (++numCalls > 3) {
                    Assertions.fail("Value serializer is called; it should never happen");
                }

                return super.serialize(topic, data);
            }
        };

        context.setValueSerde(Serdes.serdeFrom(serializer, new StringDeserializer()));
        store = createKeyValueStore(driver.context());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.delete(0);
        store.delete(1);

        // should not include deleted records in iterator
        final Map<Integer, String> expectedContents = Collections.singletonMap(2, "two");
        Assertions.assertEquals(expectedContents, getContents(store.all()));
    }

    @Test
    public void shouldDeleteIfSerializedValueIsNull() {
        store.close();

        final Serializer<String> serializer = new StringSerializer() {
            @Override
            public byte[] serialize(final String topic, final String data) {
                if (data.equals("null")) {
                    // will be serialized to null bytes, indicating deletes
                    return null;
                }
                return super.serialize(topic, data);
            }
        };

        context.setValueSerde(Serdes.serdeFrom(serializer, new StringDeserializer()));
        store = createKeyValueStore(driver.context());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(0, "null");
        store.put(1, "null");

        // should not include deleted records in iterator
        final Map<Integer, String> expectedContents = Collections.singletonMap(2, "two");
        Assertions.assertEquals(expectedContents, getContents(store.all()));
    }

    @Test
    public void testPutGetRange() {
        // Verify that the store reads and writes correctly ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        Assertions.assertEquals(5, driver.sizeOf(store));
        Assertions.assertEquals("zero", store.get(0));
        Assertions.assertEquals("one", store.get(1));
        Assertions.assertEquals("two", store.get(2));
        Assertions.assertNull(store.get(3));
        Assertions.assertEquals("four", store.get(4));
        Assertions.assertEquals("five", store.get(5));
        // Flush now so that for caching store, we will not skip the deletion following an put
        store.flush();
        store.delete(5);
        Assertions.assertEquals(4, driver.sizeOf(store));

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        Assertions.assertEquals("zero", driver.flushedEntryStored(0));
        Assertions.assertEquals("one", driver.flushedEntryStored(1));
        Assertions.assertEquals("two", driver.flushedEntryStored(2));
        Assertions.assertEquals("four", driver.flushedEntryStored(4));
        Assertions.assertNull(driver.flushedEntryStored(5));

        Assertions.assertFalse(driver.flushedEntryRemoved(0));
        Assertions.assertFalse(driver.flushedEntryRemoved(1));
        Assertions.assertFalse(driver.flushedEntryRemoved(2));
        Assertions.assertFalse(driver.flushedEntryRemoved(4));
        Assertions.assertTrue(driver.flushedEntryRemoved(5));

        final HashMap<Integer, String> expectedContents = new HashMap<>();
        expectedContents.put(2, "two");
        expectedContents.put(4, "four");

        // Check range iteration ...
        Assertions.assertEquals(expectedContents, getContents(store.range(2, 4)));
        Assertions.assertEquals(expectedContents, getContents(store.range(2, 6)));

        // Check all iteration ...
        expectedContents.put(0, "zero");
        expectedContents.put(1, "one");
        Assertions.assertEquals(expectedContents, getContents(store.all()));
    }

    @Test
    public void testPutGetReverseRange() {
        // Verify that the store reads and writes correctly ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        Assertions.assertEquals(5, driver.sizeOf(store));
        Assertions.assertEquals("zero", store.get(0));
        Assertions.assertEquals("one", store.get(1));
        Assertions.assertEquals("two", store.get(2));
        Assertions.assertNull(store.get(3));
        Assertions.assertEquals("four", store.get(4));
        Assertions.assertEquals("five", store.get(5));
        // Flush now so that for caching store, we will not skip the deletion following an put
        store.flush();
        store.delete(5);
        Assertions.assertEquals(4, driver.sizeOf(store));

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        Assertions.assertEquals("zero", driver.flushedEntryStored(0));
        Assertions.assertEquals("one", driver.flushedEntryStored(1));
        Assertions.assertEquals("two", driver.flushedEntryStored(2));
        Assertions.assertEquals("four", driver.flushedEntryStored(4));
        Assertions.assertNull(driver.flushedEntryStored(5));

        Assertions.assertFalse(driver.flushedEntryRemoved(0));
        Assertions.assertFalse(driver.flushedEntryRemoved(1));
        Assertions.assertFalse(driver.flushedEntryRemoved(2));
        Assertions.assertFalse(driver.flushedEntryRemoved(4));
        Assertions.assertTrue(driver.flushedEntryRemoved(5));

        final HashMap<Integer, String> expectedContents = new HashMap<>();
        expectedContents.put(2, "two");
        expectedContents.put(4, "four");

        // Check range iteration ...
        Assertions.assertEquals(expectedContents, getContents(store.reverseRange(2, 4)));
        Assertions.assertEquals(expectedContents, getContents(store.reverseRange(2, 6)));

        // Check all iteration ...
        expectedContents.put(0, "zero");
        expectedContents.put(1, "one");
        Assertions.assertEquals(expectedContents, getContents(store.reverseAll()));
    }

    @Test
    public void testPutGetWithDefaultSerdes() {
        // Verify that the store reads and writes correctly ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        Assertions.assertEquals(5, driver.sizeOf(store));
        Assertions.assertEquals("zero", store.get(0));
        Assertions.assertEquals("one", store.get(1));
        Assertions.assertEquals("two", store.get(2));
        Assertions.assertNull(store.get(3));
        Assertions.assertEquals("four", store.get(4));
        Assertions.assertEquals("five", store.get(5));
        store.flush();
        store.delete(5);

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        Assertions.assertEquals("zero", driver.flushedEntryStored(0));
        Assertions.assertEquals("one", driver.flushedEntryStored(1));
        Assertions.assertEquals("two", driver.flushedEntryStored(2));
        Assertions.assertEquals("four", driver.flushedEntryStored(4));
        Assertions.assertNull(driver.flushedEntryStored(5));

        Assertions.assertFalse(driver.flushedEntryRemoved(0));
        Assertions.assertFalse(driver.flushedEntryRemoved(1));
        Assertions.assertFalse(driver.flushedEntryRemoved(2));
        Assertions.assertFalse(driver.flushedEntryRemoved(4));
        Assertions.assertTrue(driver.flushedEntryRemoved(5));
    }

    @Test
    public void testRestore() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());

        // Verify that the store's contents were properly restored ...
        Assertions.assertEquals(0, driver.checkForRestoredEntries(store));

        // and there are no other entries ...
        Assertions.assertEquals(4, driver.sizeOf(store));
    }

    @Test
    public void testRestoreWithDefaultSerdes() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());
        // Verify that the store's contents were properly restored ...
        Assertions.assertEquals(0, driver.checkForRestoredEntries(store));

        // and there are no other entries ...
        Assertions.assertEquals(4, driver.sizeOf(store));
    }

    @Test
    public void testPutIfAbsent() {
        // Verify that the store reads and writes correctly ...
        Assertions.assertNull(store.putIfAbsent(0, "zero"));
        Assertions.assertNull(store.putIfAbsent(1, "one"));
        Assertions.assertNull(store.putIfAbsent(2, "two"));
        Assertions.assertNull(store.putIfAbsent(4, "four"));
        Assertions.assertEquals("four", store.putIfAbsent(4, "unexpected value"));
        Assertions.assertEquals(4, driver.sizeOf(store));
        Assertions.assertEquals("zero", store.get(0));
        Assertions.assertEquals("one", store.get(1));
        Assertions.assertEquals("two", store.get(2));
        Assertions.assertNull(store.get(3));
        Assertions.assertEquals("four", store.get(4));

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        Assertions.assertEquals("zero", driver.flushedEntryStored(0));
        Assertions.assertEquals("one", driver.flushedEntryStored(1));
        Assertions.assertEquals("two", driver.flushedEntryStored(2));
        Assertions.assertEquals("four", driver.flushedEntryStored(4));

        Assertions.assertFalse(driver.flushedEntryRemoved(0));
        Assertions.assertFalse(driver.flushedEntryRemoved(1));
        Assertions.assertFalse(driver.flushedEntryRemoved(2));
        Assertions.assertFalse(driver.flushedEntryRemoved(4));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        assertThrows(NullPointerException.class, () -> store.put(null, "anyValue"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        store.put(1, null);
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutIfAbsentNullKey() {
        assertThrows(NullPointerException.class, () -> store.putIfAbsent(null, "anyValue"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutIfAbsentNullValue() {
        store.putIfAbsent(1, null);
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPutAllNullKey() {
        assertThrows(NullPointerException.class, () -> store.putAll(Collections.singletonList(new KeyValue<>(null, "anyValue"))));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutAllNullKey() {
        store.putAll(Collections.singletonList(new KeyValue<>(1, null)));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnDeleteNullKey() {
        assertThrows(NullPointerException.class, () -> store.delete(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        assertThrows(NullPointerException.class, () -> store.get(null));
    }

    @Test
    public void shouldReturnValueOnRangeNullToKey() {
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");

        final LinkedList<KeyValue<Integer, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>(0, "zero"));
        expectedContents.add(new KeyValue<>(1, "one"));

        try (final KeyValueIterator<Integer, String> iterator = store.range(null, 1)) {
            Assertions.assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldReturnValueOnRangeKeyToNull() {
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");

        final LinkedList<KeyValue<Integer, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>(1, "one"));
        expectedContents.add(new KeyValue<>(2, "two"));

        try (final KeyValueIterator<Integer, String> iterator = store.range(1, null)) {
            Assertions.assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldReturnValueOnRangeNullToNull() {
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");

        final LinkedList<KeyValue<Integer, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>(0, "zero"));
        expectedContents.add(new KeyValue<>(1, "one"));
        expectedContents.add(new KeyValue<>(2, "two"));

        try (final KeyValueIterator<Integer, String> iterator = store.range(null, null)) {
            Assertions.assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldReturnValueOnReverseRangeNullToKey() {
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");

        final LinkedList<KeyValue<Integer, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>(1, "one"));
        expectedContents.add(new KeyValue<>(0, "zero"));

        try (final KeyValueIterator<Integer, String> iterator = store.reverseRange(null, 1)) {
            Assertions.assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldReturnValueOnReverseRangeKeyToNull() {
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");

        final LinkedList<KeyValue<Integer, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>(2, "two"));
        expectedContents.add(new KeyValue<>(1, "one"));

        try (final KeyValueIterator<Integer, String> iterator = store.reverseRange(1, null)) {
            Assertions.assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldReturnValueOnReverseRangeNullToNull() {
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");

        final LinkedList<KeyValue<Integer, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>(2, "two"));
        expectedContents.add(new KeyValue<>(1, "one"));
        expectedContents.add(new KeyValue<>(0, "zero"));

        try (final KeyValueIterator<Integer, String> iterator = store.reverseRange(null, null)) {
            Assertions.assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void testSize() {
        Assertions.assertEquals(0, store.approximateNumEntries(), "A newly created store should have no entries");

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        store.flush();
        Assertions.assertEquals(5, store.approximateNumEntries());
    }

    @Test
    public void shouldPutAll() {
        final List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "one"));
        entries.add(new KeyValue<>(2, "two"));

        store.putAll(entries);

        final List<KeyValue<Integer, String>> allReturned = new ArrayList<>();
        final List<KeyValue<Integer, String>> expectedReturned =
            Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));
        final Iterator<KeyValue<Integer, String>> iterator = store.all();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }
        assertThat(allReturned, equalTo(expectedReturned));
    }

    @Test
    public void shouldPutReverseAll() {
        final List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "one"));
        entries.add(new KeyValue<>(2, "two"));

        store.putAll(entries);

        final List<KeyValue<Integer, String>> allReturned = new ArrayList<>();
        final List<KeyValue<Integer, String>> expectedReturned =
            Arrays.asList(KeyValue.pair(2, "two"), KeyValue.pair(1, "one"));
        final Iterator<KeyValue<Integer, String>> iterator = store.reverseAll();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }
        assertThat(allReturned, equalTo(expectedReturned));
    }

    @Test
    public void shouldDeleteFromStore() {
        store.put(1, "one");
        store.put(2, "two");
        store.delete(2);
        Assertions.assertNull(store.get(2));
    }

    @Test
    public void shouldReturnSameResultsForGetAndRangeWithEqualKeys() {
        final List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "one"));
        entries.add(new KeyValue<>(2, "two"));
        entries.add(new KeyValue<>(3, "three"));

        store.putAll(entries);

        final Iterator<KeyValue<Integer, String>> iterator = store.range(2, 2);

        Assertions.assertEquals(iterator.next().value, store.get(2));
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReturnSameResultsForGetAndReverseRangeWithEqualKeys() {
        final List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "one"));
        entries.add(new KeyValue<>(2, "two"));
        entries.add(new KeyValue<>(3, "three"));

        store.putAll(entries);

        final Iterator<KeyValue<Integer, String>> iterator = store.reverseRange(2, 2);

        Assertions.assertEquals(iterator.next().value, store.get(2));
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotThrowConcurrentModificationException() {
        store.put(0, "zero");

        try (final KeyValueIterator<Integer, String> results = store.range(0, 2)) {

            store.put(1, "one");

            Assertions.assertEquals(new KeyValue<>(0, "zero"), results.next());
        }
    }

    @Test
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            try (final KeyValueIterator<Integer, String> iterator = store.range(-1, 1)) {
                Assertions.assertFalse(iterator.hasNext());
            }

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("Returning empty iterator for fetch with invalid key range: from > to." +
                    " This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes." +
                    " Note that the built-in numerical serdes do not follow this for negative numbers")
            );
        }
    }

    @Test
    public void shouldNotThrowInvalidReverseRangeExceptionWithNegativeFromKey() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            try (final KeyValueIterator<Integer, String> iterator = store.reverseRange(-1, 1)) {
                Assertions.assertFalse(iterator.hasNext());
            }

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("Returning empty iterator for fetch with invalid key range: from > to." +
                    " This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes." +
                    " Note that the built-in numerical serdes do not follow this for negative numbers")
            );
        }
    }

    @Test
    public void shouldNotThrowInvalidRangeExceptionWithFromLargerThanTo() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            try (final KeyValueIterator<Integer, String> iterator = store.range(2, 1)) {
                Assertions.assertFalse(iterator.hasNext());
            }

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("Returning empty iterator for fetch with invalid key range: from > to." +
                    " This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes." +
                    " Note that the built-in numerical serdes do not follow this for negative numbers")
            );
        }
    }

    @Test
    public void shouldNotThrowInvalidReverseRangeExceptionWithFromLargerThanTo() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            try (final KeyValueIterator<Integer, String> iterator = store.reverseRange(2, 1)) {
                Assertions.assertFalse(iterator.hasNext());
            }

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("Returning empty iterator for fetch with invalid key range: from > to." +
                    " This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes." +
                    " Note that the built-in numerical serdes do not follow this for negative numbers")
            );
        }
    }
}
