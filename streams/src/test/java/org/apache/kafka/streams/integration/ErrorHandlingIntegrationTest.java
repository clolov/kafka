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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Timeout(600)
@Tag("integration")
public class ErrorHandlingIntegrationTest {

    private final String testId;
    private final String appId;

    // Task 0
    private final String inputTopic;
    private final String outputTopic;
    // Task 1
    private final String errorInputTopic;
    private final String errorOutputTopic;

    ErrorHandlingIntegrationTest(final TestInfo testInfo) {
        testId = safeUniqueTestName(getClass(), testInfo);
        appId = "appId_" + testId;

        // Task 0
        inputTopic = "input" + testId;
        outputTopic = "output" + testId;
        // Task 1
        errorInputTopic = "error-input" + testId;
        errorOutputTopic = "error-output" + testId;
    }

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private final Properties properties = props();

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void setup() {
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, errorInputTopic, errorOutputTopic, inputTopic, outputTopic);
    }

    private Properties props() {
        return mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath()),
                mkEntry(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0),
                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 15000L),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class))
        );
    }

    @Test
    public void shouldBackOffTaskAndEmitDataWithinSameTopology() throws Exception {
        final AtomicInteger noOutputExpected = new AtomicInteger(0);
        final AtomicInteger outputExpected = new AtomicInteger(0);

        try (final KafkaStreamsNamedTopologyWrapper kafkaStreams = new KafkaStreamsNamedTopologyWrapper(properties)) {
            kafkaStreams.setUncaughtExceptionHandler(exception -> StreamThreadExceptionResponse.REPLACE_THREAD);

            final NamedTopologyBuilder builder = kafkaStreams.newNamedTopologyBuilder("topology_A");
            builder.stream(inputTopic).peek((k, v) -> outputExpected.incrementAndGet()).to(outputTopic);
            builder.stream(errorInputTopic)
                .peek((k, v) -> {
                    throw new RuntimeException("Kaboom");
                })
                .peek((k, v) -> noOutputExpected.incrementAndGet())
                .to(errorOutputTopic);

            kafkaStreams.addNamedTopology(builder.build());

            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                errorInputTopic,
                Arrays.asList(
                    new KeyValue<>(1, "A")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerDeserializer.class,
                    StringDeserializer.class
                ),
                outputTopic,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                )
            );
            assertThat(noOutputExpected.get(), equalTo(0));
            assertThat(outputExpected.get(), equalTo(2));
        }
    }
}
