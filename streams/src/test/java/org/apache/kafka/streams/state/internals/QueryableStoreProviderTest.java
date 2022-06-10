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


import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.NoOpWindowStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.apache.kafka.test.StateStoreProviderStub;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;


public class QueryableStoreProviderTest {

    private final String keyValueStore = "key-value";
    private final String windowStore = "window-store";
    private QueryableStoreProvider storeProvider;
    private HashMap<String, StateStore> globalStateStores;
    private final int numStateStorePartitions = 2;

    @BeforeEach
    public void before() {
        final StateStoreProviderStub theStoreProvider = new StateStoreProviderStub(false);
        for (int partition = 0; partition < numStateStorePartitions; partition++) {
            theStoreProvider.addStore(keyValueStore, partition, new NoOpReadOnlyStore<>());
            theStoreProvider.addStore(windowStore, partition, new NoOpWindowStore());
        }
        globalStateStores = new HashMap<>();
        storeProvider =
            new QueryableStoreProvider(
                new GlobalStateStoreProvider(globalStateStores)
            );
        storeProvider.addStoreProviderForThread("thread1", theStoreProvider);
    }

    @Test
    public void shouldThrowExceptionIfKVStoreDoesntExist() {
        Assertions.assertThrows(InvalidStateStoreException.class, () -> storeProvider.getStore(
            StoreQueryParameters.fromNameAndType("not-a-store", QueryableStoreTypes.keyValueStore())).get("1"));
    }

    @Test
    public void shouldThrowExceptionIfWindowStoreDoesntExist() {
        Assertions.assertThrows(InvalidStateStoreException.class, () -> storeProvider.getStore(
            StoreQueryParameters.fromNameAndType("not-a-store", QueryableStoreTypes.windowStore())).fetch("1", System.currentTimeMillis()));
    }

    @Test
    public void shouldReturnKVStoreWhenItExists() {
        Assertions.assertNotNull(storeProvider.getStore(StoreQueryParameters.fromNameAndType(keyValueStore, QueryableStoreTypes.keyValueStore())));
    }

    @Test
    public void shouldReturnWindowStoreWhenItExists() {
        Assertions.assertNotNull(storeProvider.getStore(StoreQueryParameters.fromNameAndType(windowStore, QueryableStoreTypes.windowStore())));
    }

    @Test
    public void shouldThrowExceptionWhenLookingForWindowStoreWithDifferentType() {
        Assertions.assertThrows(InvalidStateStoreException.class, () -> storeProvider.getStore(StoreQueryParameters.fromNameAndType(windowStore,
            QueryableStoreTypes.keyValueStore())).get("1"));
    }

    @Test
    public void shouldThrowExceptionWhenLookingForKVStoreWithDifferentType() {
        Assertions.assertThrows(InvalidStateStoreException.class, () -> storeProvider.getStore(StoreQueryParameters.fromNameAndType(keyValueStore,
            QueryableStoreTypes.windowStore())).fetch("1", System.currentTimeMillis()));
    }

    @Test
    public void shouldFindGlobalStores() {
        globalStateStores.put("global", new NoOpReadOnlyStore<>());
        Assertions.assertNotNull(storeProvider.getStore(StoreQueryParameters.fromNameAndType("global", QueryableStoreTypes.keyValueStore())));
    }

    @Test
    public void shouldReturnKVStoreWithPartitionWhenItExists() {
        Assertions.assertNotNull(storeProvider.getStore(StoreQueryParameters.fromNameAndType(keyValueStore, QueryableStoreTypes.keyValueStore()).withPartition(numStateStorePartitions - 1)));
    }

    @Test
    public void shouldThrowExceptionWhenKVStoreWithPartitionDoesntExists() {
        final int partition = numStateStorePartitions + 1;
        final InvalidStateStoreException thrown = Assertions.assertThrows(InvalidStateStoreException.class, () ->
                storeProvider.getStore(
                        StoreQueryParameters
                                .fromNameAndType(keyValueStore, QueryableStoreTypes.keyValueStore())
                                .withPartition(partition)).get("1")
        );
        assertThat(thrown.getMessage(), equalTo(String.format("The specified partition %d for store %s does not exist.", partition, keyValueStore)));
    }

    @Test
    public void shouldReturnWindowStoreWithPartitionWhenItExists() {
        Assertions.assertNotNull(storeProvider.getStore(StoreQueryParameters.fromNameAndType(windowStore, QueryableStoreTypes.windowStore()).withPartition(numStateStorePartitions - 1)));
    }

    @Test
    public void shouldThrowExceptionWhenWindowStoreWithPartitionDoesntExists() {
        final int partition = numStateStorePartitions + 1;
        final InvalidStateStoreException thrown = Assertions.assertThrows(InvalidStateStoreException.class, () ->
                storeProvider.getStore(
                        StoreQueryParameters
                                .fromNameAndType(windowStore, QueryableStoreTypes.windowStore())
                                .withPartition(partition)).fetch("1", System.currentTimeMillis())
        );
        assertThat(thrown.getMessage(), equalTo(String.format("The specified partition %d for store %s does not exist.", partition, windowStore)));
    }
}