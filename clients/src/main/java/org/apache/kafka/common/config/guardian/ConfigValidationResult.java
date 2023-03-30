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

public class ConfigValidationResult<T> {
    private final T enforcedValue;
    private final String reason;
    private final boolean accepted;

    private ConfigValidationResult(T enforcedValue, String reason, boolean accepted) {
        this.enforcedValue = enforcedValue;
        this.reason = reason;
        this.accepted = accepted;
    }

    public T getEnforcedValue() {
        return enforcedValue;
    }

    public String getReason() {
        return reason;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public static <T> ConfigValidationResult<T> accepted(T value) {
        return new ConfigValidationResult<>(value, null, true);
    }

    public static <T> ConfigValidationResult<T> enforced(T value) {
        return new ConfigValidationResult<>(value, null, true);
    }

    public static <T> ConfigValidationResult<T> rejected(T value, String reason) {
        return new ConfigValidationResult<>(value, reason, false);
    }
}
