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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public class DynamicRemoteLogConfig implements BrokerReconfigurable {

    private static final Logger log = LoggerFactory.getLogger(DynamicRemoteLogConfig.class);

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
            RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP,
            RemoteLogManagerConfig.REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP);

    private static final Set<String> BYTES_CONFIGS = Set.of(
            RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP);

    private static final Set<String> THREAD_COUNT_CONFIGS = Set.of(
            RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP);

    private final Supplier<AbstractKafkaConfig> currentConfig;
    private final Optional<RemoteLogManager> remoteLogManager;

    public DynamicRemoteLogConfig(Supplier<AbstractKafkaConfig> currentConfig, Optional<RemoteLogManager> remoteLogManager) {
        this.currentConfig = currentConfig;
        this.remoteLogManager = remoteLogManager;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(AbstractKafkaConfig newConfig) {
        AbstractKafkaConfig config = currentConfig.get();
        for (Map.Entry<String, ?> entry : newConfig.values().entrySet()) {
            String k = entry.getKey();
            Object v = entry.getValue();
            if (BYTES_CONFIGS.contains(k)) {
                long newValue = (Long) v;
                long oldValue = config.getLong(k);
                if (newValue != oldValue && newValue <= 0) {
                    String errorMsg = "Dynamic remote log manager config update validation failed for " + k + "=" + v;
                    throw new ConfigException(errorMsg + ", value should be at least 1");
                }
            }

            if (THREAD_COUNT_CONFIGS.contains(k)) {
                int newValue = (Integer) v;
                // This logic preserves backward compatibility in scenarios where
                // `remote.log.manager.thread.pool.size` is configured in config file,
                // but `remote.log.manager.follower.thread.pool.size` is set dynamically.
                // This can be removed once `remote.log.manager.thread.pool.size` is removed.
                int oldValue = RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP.equals(k)
                    ? config.remoteLogManagerConfig().remoteLogManagerFollowerThreadPoolSize()
                    : config.getInt(k);
                if (newValue != oldValue) {
                    String errorMsg = "Dynamic thread count update validation failed for " + k + "=" + v;
                    if (newValue <= 0)
                        throw new ConfigException(errorMsg + ", value should be at least 1");
                    if (newValue < oldValue / 2)
                        throw new ConfigException(errorMsg + ", value should be at least half the current value " + oldValue);
                    if (newValue > oldValue * 2)
                        throw new ConfigException(errorMsg + ", value should not be greater than double the current value " + oldValue);
                }
            }
        }
    }

    @Override
    public void reconfigure(AbstractKafkaConfig oldConfig, AbstractKafkaConfig newConfig) {
        remoteLogManager.ifPresent(rlm -> {
            String cacheSizeProp = RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP;
            if (isChangedLongValue(oldConfig, newConfig, cacheSizeProp)) {
                rlm.resizeCacheSize(newConfig.getLong(cacheSizeProp));
                logUpdate(oldConfig, newConfig, cacheSizeProp);
            }
            String copyQuotaProp = RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP;
            if (isChangedLongValue(oldConfig, newConfig, copyQuotaProp)) {
                rlm.updateCopyQuota(newConfig.getLong(copyQuotaProp));
                logUpdate(oldConfig, newConfig, copyQuotaProp);
            }
            String fetchQuotaProp = RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP;
            if (isChangedLongValue(oldConfig, newConfig, fetchQuotaProp)) {
                rlm.updateFetchQuota(newConfig.getLong(fetchQuotaProp));
                logUpdate(oldConfig, newConfig, fetchQuotaProp);
            }

            RemoteLogManagerConfig newRLMConfig = newConfig.remoteLogManagerConfig();
            RemoteLogManagerConfig oldRLMConfig = oldConfig.remoteLogManagerConfig();
            if (newRLMConfig.remoteLogManagerCopierThreadPoolSize() != oldRLMConfig.remoteLogManagerCopierThreadPoolSize())
                rlm.resizeCopierThreadPool(newRLMConfig.remoteLogManagerCopierThreadPoolSize());

            if (newRLMConfig.remoteLogManagerExpirationThreadPoolSize() != oldRLMConfig.remoteLogManagerExpirationThreadPoolSize())
                rlm.resizeExpirationThreadPool(newRLMConfig.remoteLogManagerExpirationThreadPoolSize());

            if (newRLMConfig.remoteLogManagerFollowerThreadPoolSize() != oldRLMConfig.remoteLogManagerFollowerThreadPoolSize())
                rlm.resizeFollowerThreadPool(newRLMConfig.remoteLogManagerFollowerThreadPoolSize());

            if (newRLMConfig.remoteLogReaderThreads() != oldRLMConfig.remoteLogReaderThreads())
                rlm.resizeReaderThreadPool(newRLMConfig.remoteLogReaderThreads());
        });
    }

    private static boolean isChangedLongValue(AbstractKafkaConfig oldConfig, AbstractKafkaConfig newConfig, String name) {
        return !oldConfig.getLong(name).equals(newConfig.getLong(name));
    }

    private static void logUpdate(AbstractKafkaConfig oldConfig, AbstractKafkaConfig newConfig, String name) {
        log.info("Dynamic remote log manager config: {} updated, old value: {}, new value: {}",
            name, oldConfig.getLong(name), newConfig.getLong(name));
    }
}
