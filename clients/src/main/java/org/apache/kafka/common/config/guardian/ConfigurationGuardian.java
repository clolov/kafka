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

import java.util.Collection;
import org.apache.kafka.common.Configurable;

/*
 * Note for cluster administrators:
 * Direct Zookeeper updates can bypass client-side validation, leading to a successful request even if Brokers reject the update
 * (workaround: disable external access to Zookeeper)
 *
 */
public interface ConfigurationGuardian extends Configurable {
    /*
     * Supports API AlterClientQuotas.
     * Valid entity types: 'client-id', 'user', and 'ip'
     */
    ConfigValidationResult<Double> validateQuota(Collection<ConfigEntity> entities, OperationType operationType, String configName, Double quotaValue);

    /*
     * Supports API IncrementalAlterConfigs/AlterConfig/CreateTopics
     * Valid entity types: 'topics', 'brokers', 'broker-loggers'
     */
    ConfigValidationResult<String> validateConfig(ConfigEntity entity, OperationType operationType, String configName, String configValue);

    /*
     * Shortcut for API such as AlterConfig and CreateTopics
     */
    default ConfigValidationResult<String> validateConfig(ConfigEntity entity, String configName, String configValue) {
        return validateConfig(entity, OperationType.SET, configName, configValue);
    }
}


