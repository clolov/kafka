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

package org.apache.kafka.common.config.guardian;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultConfigurationGuardianTest {
    private ConfigurationGuardian configurationGuardian;

    @BeforeEach
    void setup() {
        this.configurationGuardian = new DefaultConfigurationGuardian();
    }

    @ParameterizedTest(name = "resource: {1}, config: {2}, value: {3}, target value: {5}")
    @MethodSource(value = "configurationInputs")
    void testValidateConfig(Map<String, ?> config, ConfigEntity resource, OperationType opType, String configName, Object configValue, boolean accepted, Object targetValue) {
        configurationGuardian.configure(config);

        ConfigValidationResult<?> result = configurationGuardian.validateConfig(resource, opType, configName, configValue.toString());
        assertEquals(accepted, result.isAccepted());

        if (accepted) {
            assertEquals(targetValue.toString(), result.getEnforcedValue());
        }
    }

    @ParameterizedTest(name = "resource: {1}, config: {2}, value: {3}, target value: {5}")
    @MethodSource(value = "quotaInputs")
    void testValidateQuota(Map<String, ?> config, Collection<ConfigEntity> entities, OperationType opType, String configName, Double quotaValue, boolean accepted, Double targetValue) {
        configurationGuardian.configure(config);

        ConfigValidationResult<?> result = configurationGuardian.validateQuota(entities, opType, configName, quotaValue);
        assertEquals(accepted, result.isAccepted());

        if (accepted) {
            assertEquals(targetValue, result.getEnforcedValue());
        }
    }

    private static Stream<Arguments> configurationInputs() {
        Map<String, Object> config = new HashMap<>();
        // Restrict
        config.put("guard.topics.restricted.ms[all]", "restrict");

        // Range
        config.put("guard.topics.range-enforce.ms[set]", "enforce[500 : 10000]");
        config.put("guard.topics.range-fail.ms[set]", "allow[500 : 10000]");

        // Enum
        config.put("guard.topics.enum-fail.ms[set]", "allow[alpha, beta, gamma]");

        return Stream.of(
                // Fail open
                Arguments.of(config, topic(), OperationType.SET, "fail.open", "any", true, "any"),

                // Restrict
                Arguments.of(config, topic(), OperationType.SET, "restricted.ms", "any", false, null),

                // Enforce: Range
                Arguments.of(config, topic(), OperationType.SET, "range-enforce.ms", "1000", true, "1000"),
                Arguments.of(config, topic(), OperationType.SET, "range-enforce.ms", "100", true, "500"),
                Arguments.of(config, topic(), OperationType.SET, "range-enforce.ms", "20000", true, "10000"),

                // Allow: Range
                Arguments.of(config, topic(), OperationType.SET, "range-fail.ms", "1000", true, "1000"),
                Arguments.of(config, topic(), OperationType.SET, "range-fail.ms", "100", false, null),
                Arguments.of(config, topic(), OperationType.SET, "range-fail.ms", "10000", true, "10000"),
                Arguments.of(config, topic(), OperationType.SET, "range-fail.ms", "20000", false, null),

                // Allow: Enum
                Arguments.of(config, topic(), OperationType.SET, "enum-fail.ms", "alpha", true, "alpha"),
                Arguments.of(config, topic(), OperationType.SET, "enum-fail.ms", "beta", true, "beta"),
                Arguments.of(config, topic(), OperationType.SET, "enum-fail.ms", "gamma", true, "gamma"),
                Arguments.of(config, topic(), OperationType.SET, "enum-fail.ms", "delta", false, null)
        );
    }

    private static Stream<Arguments> quotaInputs() {
        Map<String, Object> config = new HashMap<>();
        config.put("guard.client-id.produce_rate[set]", "enforce[500.1 : 10000.1]");
        config.put("guard.user.produce_rate[set]", "enforce[500.2 : 10000.2]");
        config.put("guard.client-id.user.produce_rate[set]", "enforce[500.3 : 10000.3]");

        return Stream.of(
                // Enforce: Range client
                Arguments.of(config, clients(), OperationType.SET, "produce_rate", 1000d, true, 1000d),
                Arguments.of(config, clients(), OperationType.SET, "produce_rate", 100d, true, 500.1d),
                Arguments.of(config, clients(), OperationType.SET, "produce_rate", 20000d, true, 10000.1d),

                // Enforce: Range user
                Arguments.of(config, users(), OperationType.SET, "produce_rate", 1000d, true, 1000d),
                Arguments.of(config, users(), OperationType.SET, "produce_rate", 100d, true, 500.2d),
                Arguments.of(config, users(), OperationType.SET, "produce_rate", 20000d, true, 10000.2d),

                // Enforce: Range client-user
                Arguments.of(config, clientUsers(), OperationType.SET, "produce_rate", 1000d, true, 1000d),
                Arguments.of(config, clientUsers(), OperationType.SET, "produce_rate", 100d, true, 500.3d),
                Arguments.of(config, clientUsers(), OperationType.SET, "produce_rate", 20000d, true, 10000.3d)
        );
    }

    private static ConfigEntity topic() {
        return new ConfigEntity(new ConfigResource(ConfigResource.Type.TOPIC, "topic"));
    }

    private static Collection<ConfigEntity> users() {
        List<ConfigEntity> output = new ArrayList<>();
        output.add(new ConfigEntity("user", "user-1"));
        return output;
    }

    private static Collection<ConfigEntity> clients() {
        List<ConfigEntity> output = new ArrayList<>();
        output.add(new ConfigEntity("client-id", "client-1"));
        return output;
    }

    private static Collection<ConfigEntity> clientUsers() {
        List<ConfigEntity> output = new ArrayList<>();
        output.add(new ConfigEntity("user", "user-1"));
        output.add(new ConfigEntity("client-id", "client-1"));
        return output;
    }
}
