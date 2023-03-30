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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConfigurationGuardian implements ConfigurationGuardian {
    private static final Logger log = LoggerFactory.getLogger(DefaultConfigurationGuardian.class);
    private static final Rule RULE_FALLBACK = ConfigValidationResult::accepted;
    private static final Rule RULE_INTERVAL_FAILURE = new Rule() {
        @Override
        public <T> ConfigValidationResult<T> check(T value) {
            return ConfigValidationResult.rejected(value, "Internal error, check server logs");
        }
    };

    private Map<String, Object> configs;

    @Override
    public void configure(Map<String, ?> configs) {
        // This is the best place to validate provided rules syntax.
        // guard.<topics|brokers|etc>.<configuration name>[set|append|delete|subtract] = <allow|enforce|restrict> [min:max OR value1,value2,value3]
        this.configs = Collections.unmodifiableMap(configs);
    }

    @Override
    public ConfigValidationResult<Double> validateQuota(Collection<ConfigEntity> entities, OperationType operationType, String configName, Double quotaValue) {
        // The default implementation constructs the path based on the alphabetical order of the entities,
        // for example:
        //  input:  [entity_type = "user", entity_type = "client-id"]
        //  output: "client-id.user"
        String entityPath = entities.stream().map(ConfigEntity::getEntityType).sorted().collect(Collectors.joining("."));
        return validate(entityPath, operationType, configName, quotaValue);
    }

    @Override
    public ConfigValidationResult<String> validateConfig(ConfigEntity entity, OperationType operationType, String configName, String configValue) {
        return validate(entity.getEntityType(), operationType, configName, configValue);
    }

    private <T> ConfigValidationResult<T> validate(String entityPath, OperationType operationType, String configName, T configValue) {
        // First checking 'all' operation
        String configKeyAll = buildKey(OperationType.ALL, entityPath, configName);
        Object rawValue = this.configs.getOrDefault(configKeyAll, this.configs.get(buildKey(operationType, entityPath, configName)));

        // Accepting if no rule provided
        if (null == rawValue) {
            return ConfigValidationResult.accepted(configValue);
        }

        return build(configName, rawValue).check(configValue);
    }

    private Rule build(String configName, Object raw) {
        String sanitizedString = raw.toString().replaceAll("\\s", "");
        StringTokenizer tokenizer = new StringTokenizer(sanitizedString, "[]:,'", true);

        if (!tokenizer.hasMoreElements()) {
            return RULE_FALLBACK;
        }

        String enforcementToken = tokenizer.nextToken();
        EnforcementType enforcement;
        try {
            enforcement = EnforcementType.valueOf(enforcementToken.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            log.error("Can't parse rule '{}'", sanitizedString, e);
            return RULE_INTERVAL_FAILURE;
        }

        if (EnforcementType.RESTRICT == enforcement) {
            return new Rule() {
                @Override
                public <T> ConfigValidationResult<T> check(T value) {
                    return ConfigValidationResult.rejected(value, "Modification of the '" + configName + "' is not permitted");
                }
            };
        }

        return parseRule(configName, tokenizer, enforcement);
    }

    private Rule parseRule(String configName, StringTokenizer tokenizer, EnforcementType enforcement) {
        List<String> values = new ArrayList<>();

        boolean enumList = true;
        boolean minValueProvided = false;
        while (tokenizer.hasMoreElements()) {
            String token = tokenizer.nextToken();
            switch (token) {
                case "[":
                case "]": {
                    break;
                }

                case ":": {
                    minValueProvided = !values.isEmpty();
                    enumList = false; // range
                    break;
                }

                case "'": {
                    values.add(tokenizer.nextToken("'"));
                    tokenizer.nextToken(); // skipping closing quote
                    break;
                }

                default: {
                    values.add(token);
                    break;
                }
            }
        }

        if (enumList) {
            return new RuleEnum(configName, new HashSet<>(values));
        }

        if (values.isEmpty()) { // [-inf : inf+]
            return RULE_FALLBACK;
        }

        return new RangeRule(
                configName,
                enforcement == EnforcementType.ENFORCE,
                minValueProvided ? parseNumber(values.get(0)) : Long.MIN_VALUE,
                !minValueProvided ? parseNumber(values.get(0)) : (values.size() > 1
                        ? parseNumber(values.get(1)) // [min : max]
                        : parseNumber(values.get(0)) // [ : max]
                )
        );
    }

    private String buildKey(OperationType operationType, String entityPath, String configName) {
        log.debug("building key from op: {}, res: {}, config: {}", operationType, entityPath, configName);
        return String.format("guard.%s.%s[%s]",
                entityPath,
                configName,
                operationType.name().toLowerCase(Locale.ROOT)
        );
    }

    private enum EnforcementType {
        ENFORCE, ALLOW, RESTRICT
    }

    // Rules
    private interface Rule {
        <T> ConfigValidationResult<T> check(T value);
    }

    private static abstract class BaseRule implements Rule {
        final String configName;

        public BaseRule(String configName) {
            this.configName = configName;
        }
    }

    private static class RuleEnum extends BaseRule {
        private final Set<?> allowList;

        public RuleEnum(String configName, Set<?> allowList) {
            super(configName);
            this.allowList = allowList;
        }

        @Override
        public <T> ConfigValidationResult<T> check(T value) {
            if (allowList.contains(value)) {
                return ConfigValidationResult.accepted(value);
            }
            return ConfigValidationResult.rejected(value, String.format(
                    "Invalid value provided for the '%s' parameter. " +
                            "The allowed values are: %s. " +
                            "Please update the parameter value and try again.",
                    value, allowList));
        }
    }

    private static class RangeRule extends BaseRule {
        protected final boolean enforce;
        private final Number min;
        private final Number max;

        public RangeRule(String configName, boolean enforce, Number min, Number max) {
            super(configName);
            this.enforce = enforce;
            this.min = min;
            this.max = max;
        }

        @Override
        public <T> ConfigValidationResult<T> check(T value) {
            try {
                Number parsedValue = parseNumber(value.toString());

                if (parsedValue.doubleValue() >= min.doubleValue() && parsedValue.doubleValue() <= max.doubleValue()) {
                    return ConfigValidationResult.accepted(value);
                }

                if (!this.enforce) {
                    return ConfigValidationResult.rejected(value, String.format(
                            "The provided value for the '%s' parameter is outside of the allowed range. " +
                                    "The parameter value should be between %s and %s. " +
                                    "Please update the parameter value and try again",
                            this.configName, min, max));
                }

                return ConfigValidationResult.enforced(sanitizeType(
                        value, parsedValue.doubleValue() < min.doubleValue() ? min : max
                ));
            } catch (NumberFormatException e) {
                return ConfigValidationResult.rejected(value, String.format(
                        "Error: Invalid value for parameter '%s'. Expected a numeric value.",
                        this.configName));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T sanitizeType(T original, Number target) {
        if (original instanceof Number) {
            return (T) target;
        }

        return (T) String.valueOf(target);
    }

    private static Number parseNumber(String input) {
        try {
            return Long.parseLong(input);
        } catch (NumberFormatException e) {
            return Double.parseDouble(input);
        }
    }
}
